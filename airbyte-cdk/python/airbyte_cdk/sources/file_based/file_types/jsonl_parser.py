#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import io
import json
import logging
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple, Union

import orjson

from airbyte_cdk.sources.file_based.config.file_based_stream_config import FileBasedStreamConfig
from airbyte_cdk.sources.file_based.config.jsonl_format import JsonlFormat
from airbyte_cdk.sources.file_based.exceptions import FileBasedSourceError, RecordParseError
from airbyte_cdk.sources.file_based.file_based_stream_reader import (
    AbstractFileBasedStreamReader,
    FileReadMode,
)
from airbyte_cdk.sources.file_based.file_types.file_type_parser import FileTypeParser
from airbyte_cdk.sources.file_based.remote_file import RemoteFile
from airbyte_cdk.sources.file_based.schema_helpers import (
    PYTHON_TYPE_MAPPING,
    SchemaType,
    merge_schemas,
)


class JsonlParser(FileTypeParser):
    MAX_BYTES_PER_FILE_FOR_SCHEMA_INFERENCE = 1_000_000
    ENCODING = "utf8"

    def check_config(self, config: FileBasedStreamConfig) -> Tuple[bool, Optional[str]]:
        """
        JsonlParser does not require config checks, implicit pydantic validation is enough.
        """
        return True, None

    async def infer_schema(
        self,
        config: FileBasedStreamConfig,
        file: RemoteFile,
        stream_reader: AbstractFileBasedStreamReader,
        logger: logging.Logger,
    ) -> SchemaType:
        """
        Infers the schema for the file by inferring the schema for each line, and merging
        it with the previously-inferred schema.
        """
        inferred_schema: Mapping[str, Any] = {}

        config_format = _extract_format(config)
        rows_to_skip = (
            config_format.skip_rows_before_payload
        )

        for entry in self._parse_jsonl_entries(file, stream_reader, logger, read_limit=True, rows_to_skip=rows_to_skip, config_format=config_format):
            line_schema = self._infer_schema_for_record(entry)
            inferred_schema = merge_schemas(inferred_schema, line_schema)

        return inferred_schema

    def parse_records(
        self,
        config: FileBasedStreamConfig,
        file: RemoteFile,
        stream_reader: AbstractFileBasedStreamReader,
        logger: logging.Logger,
        discovered_schema: Optional[Mapping[str, SchemaType]],
    ) -> Iterable[Dict[str, Any]]:
        """
        This code supports parsing json objects over multiple lines even though this does not align with the JSONL format. This is for
        backward compatibility reasons i.e. the previous source-s3 parser did support this. The drawback is:
        * performance as the way we support json over multiple lines is very brute forced
        * given that we don't have `newlines_in_values` config to scope the possible inputs, we might parse the whole file before knowing if
          the input is improperly formatted or if the json is over multiple lines

        The goal is to run the V4 of source-s3 in production, track the warning log emitted when there are multiline json objects and
        deprecate this feature if it's not a valid use case.
        """
        config_format = _extract_format(config)
        rows_to_skip = (
            config_format.skip_rows_before_payload
        )

        yield from self._parse_jsonl_entries(file, stream_reader, logger, rows_to_skip=rows_to_skip, config_format=config_format)

    @classmethod
    def _infer_schema_for_record(cls, record: Dict[str, Any]) -> Dict[str, Any]:
        record_schema = {}
        for key, value in record.items():
            if value is None:
                record_schema[key] = {"type": "null"}
            else:
                record_schema[key] = {"type": PYTHON_TYPE_MAPPING[type(value)]}

        return record_schema

    @property
    def file_read_mode(self) -> FileReadMode:
        return FileReadMode.READ

    def _parse_jsonl_entries(
        self,
        file: RemoteFile,
        stream_reader: AbstractFileBasedStreamReader,
        logger: logging.Logger,
        read_limit: bool = False,
        rows_to_skip: int = 0,
        config_format: Optional[JsonlFormat] = None,
    ) -> Iterable[Dict[str, Any]]:
        # Handle XML conversion if needed
        should_convert_xml = (
            config_format and 
            config_format.convert_from_xml and 
            self._is_xml_file(file)
        )
        
        if should_convert_xml:
            # Open as binary to handle XML conversion
            with stream_reader.open_file(file, FileReadMode.READ_BINARY, None, logger) as binary_fp:
                converted_fp = self._convert_xml_content(binary_fp)
                fp = io.TextIOWrapper(converted_fp, encoding=self.ENCODING)
        else:
            fp = stream_reader.open_file(file, self.file_read_mode, self.ENCODING, logger)
            
        with fp:
            read_bytes = 0

            had_json_parsing_error = False
            has_warned_for_multiline_json_object = False
            yielded_at_least_once = False
            
            # Skip the specified number of lines from the beginning
            lines_skipped = 0
            if rows_to_skip > 0:
                for _ in range(rows_to_skip):
                    try:
                        next(fp)
                        lines_skipped += 1
                    except StopIteration:
                        # File has fewer lines than rows_to_skip
                        break

            accumulator = None
            for line in fp:
                if not accumulator:
                    accumulator = self._instantiate_accumulator(line)
                read_bytes += len(line)
                accumulator += line  # type: ignore [operator]  # In reality, it's either bytes or string and we add the same type
                try:
                    record = orjson.loads(accumulator)
                    if had_json_parsing_error and not has_warned_for_multiline_json_object:
                        logger.warning(
                            f"File at {file.uri} is using multiline JSON. Performance could be greatly reduced"
                        )
                        has_warned_for_multiline_json_object = True

                    yield record
                    yielded_at_least_once = True
                    accumulator = self._instantiate_accumulator(line)
                except orjson.JSONDecodeError:
                    had_json_parsing_error = True

                if (
                    read_limit
                    and yielded_at_least_once
                    and read_bytes >= self.MAX_BYTES_PER_FILE_FOR_SCHEMA_INFERENCE
                ):
                    logger.warning(
                        f"Exceeded the maximum number of bytes per file for schema inference ({self.MAX_BYTES_PER_FILE_FOR_SCHEMA_INFERENCE}). "
                        f"Inferring schema from an incomplete set of records."
                    )
                    break

            if had_json_parsing_error and not yielded_at_least_once:
                raise RecordParseError(
                    FileBasedSourceError.ERROR_PARSING_RECORD, filename=file.uri, lineno=line
                )

    @staticmethod
    def _instantiate_accumulator(line: Union[bytes, str]) -> Union[bytes, str]:
        if isinstance(line, bytes):
            return bytes("", json.detect_encoding(line))
        elif isinstance(line, str):
            return ""


    def _is_xml_file(self, file: RemoteFile) -> bool:
        """
        Check if the file has XML extension.
        
        Args:
            file: The RemoteFile to check
            
        Returns:
            True if file has .xml extension, False otherwise
        """
        file_path = Path(file.uri)
        file_extension = file_path.suffix.lower().lstrip('.')
        return file_extension == 'xml'
        
    def _convert_xml_content(self, binary_data: io.BytesIO) -> io.BytesIO:
        """
        Convert XML binary data to JSON format.
        
        Args:
            binary_data: Binary data containing XML content
            
        Returns:
            BytesIO object containing JSON data
        """
        def xml_to_dict(element):
            """Convert XML element to dictionary"""
            result = {}
            
            # Add attributes with @ prefix
            if element.attrib:
                for key, value in element.attrib.items():
                    result[f"@{key}"] = value
            
            # Add text content
            if element.text and element.text.strip():
                if len(element) == 0:  # Leaf node
                    return element.text.strip()
                else:
                    result["#text"] = element.text.strip()
            
            # Add child elements
            for child in element:
                child_data = xml_to_dict(child)
                
                if child.tag in result:
                    # Convert to list if multiple children with same tag
                    if not isinstance(result[child.tag], list):
                        result[child.tag] = [result[child.tag]]
                    result[child.tag].append(child_data)
                else:
                    result[child.tag] = child_data
            
            return result if result else None
        
        def dumper(obj):
            """JSON serializer for objects not serializable by default json code"""
            try:
                return obj.toJSON()
            except:
                return str(obj)
        
        # Convert xml to json
        try:
            xml_content = binary_data.read().decode('utf-8')
        except UnicodeDecodeError:
            binary_data.seek(0)
            xml_content = binary_data.read().decode('utf-8-sig')
        
        # Parse XML
        try:
            root = ET.fromstring(xml_content)
            json_file = {root.tag: xml_to_dict(root)}
        except ET.ParseError as e:
            raise RecordParseError(
                FileBasedSourceError.ERROR_PARSING_RECORD, 
                filename="XML content", 
                lineno=f"XML Parse Error: {str(e)}"
            )
        
        # Exceptions - remove unwanted attributes
        try:
            # Remove @version key if it exists
            if "ParLevelUpdates" in json_file:
                if isinstance(json_file["ParLevelUpdates"], dict):
                    json_file["ParLevelUpdates"].pop("@version", None)
        except:
            pass

        json_data = json.dumps(json_file, default=dumper)   
        return io.BytesIO(json_data.encode('utf-8'))


def _extract_format(config: FileBasedStreamConfig) -> JsonlFormat:
    config_format = config.format
    if not isinstance(config_format, JsonlFormat):
        raise ValueError(f"Invalid format config: {config_format}")
    return config_format
