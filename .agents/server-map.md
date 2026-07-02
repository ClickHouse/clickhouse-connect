# ClickHouse Server Navigation Map

Source tree lives at `.server-src/` (gitignored). The version actually being investigated is recorded in `.server-ref`.

## How to use this map

This map is intentionally version-agnostic. ClickHouse's high-level layout (`src/IO/`, `src/DataTypes/`, `src/Formats/`, `src/Core/Settings.cpp`, `IDataType`, the `Serialization*` family, the format and compression factories) has been stable for years, so the entries below should apply across most modern server tags. The long tail drifts: newly added types, renamed settings, occasional file splits during refactors.

Paths are relative to `.server-src/`. Before relying on a specific file or class pointer, sanity-check that it still exists at the tag in `.server-ref`. If a cited path is missing, tell the user the map is out of date for that area and suggest updating the map rather than guessing a replacement.

Cite file and class or function names in your answers. Do not cite line numbers, they rot.

## Native protocol (TCP wire format)

- **Packet types and protocol constants**: `src/Core/Protocol.h` defines `Protocol::Client::Enum`, `Protocol::Server::Enum`, `Protocol::Compression`, and version constants for client/server handshake and Hello/Query/Data/Exception/EndOfStream framing.
- **TCP handler**: `src/Server/TCPHandler.cpp`, `src/Server/TCPHandler.h` implement the main packet loop, handshake, per-query state, and Data/Exception/Progress packet emission.
- **Block read/write over the wire**: `src/Formats/NativeReader.cpp`, `src/Formats/NativeWriter.cpp`. Native format is the payload inside Data packets: column count, then for each column name, type string, and serialized values.
- **Compressed transport wrapping**: `src/Compression/CompressedReadBuffer.cpp`, `src/Compression/CompressedWriteBuffer.cpp`.

## Type system and binary serialization

- **Core type interface**: `src/DataTypes/IDataType.h`. Every SQL type derives from `IDataType` and exposes metadata plus a serialization object.
- **Serialization interface**: `src/DataTypes/Serializations/ISerialization.h`. One type can have multiple serializations (default, sparse, subcolumn). Binary read/write lives on methods like `serializeBinaryBulk*` / `deserializeBinaryBulk*`.
- **Per-type implementations**: `src/DataTypes/DataTypeXXX.{cpp,h}` for each type.
- **Per-type serializations**: `src/DataTypes/Serializations/SerializationXXX.{cpp,h}`.
- **Type factory and text parsing of type strings**: `src/DataTypes/DataTypeFactory.cpp`.
- **Schema binary encoding** (used when transmitting types, e.g. Dynamic): `src/DataTypes/DataTypesBinaryEncoding.{cpp,h}`.

## Specific types

### Nullable

`src/DataTypes/DataTypeNullable.{cpp,h}`, `src/DataTypes/Serializations/SerializationNullable.{cpp,h}`. Wire layout: null byte-mask (UInt8 per row, 0 for value present, 1 for NULL), then the nested column with all rows (null rows hold an undefined value).

### LowCardinality

`src/DataTypes/DataTypeLowCardinality.{cpp,h}`, `src/DataTypes/DataTypeLowCardinalityHelpers.cpp`, `src/DataTypes/Serializations/SerializationLowCardinality.{cpp,h}`. Wire layout has a multi-part header (version flags, index type) plus a dictionary of unique values and an indexes column (UInt8/UInt16/UInt32/UInt64 depending on dictionary size). Dictionary ordering is not stable across blocks; read the source before assuming.

### Array

`src/DataTypes/DataTypeArray.{cpp,h}`, `src/DataTypes/Serializations/SerializationArray.{cpp,h}`, `src/DataTypes/Serializations/SerializationArrayOffsets.{cpp,h}`. Wire layout: offsets column (UInt64, cumulative end-offsets per row), then the flattened element column.

### Tuple and Nested

`src/DataTypes/DataTypeTuple.{cpp,h}`, `src/DataTypes/Serializations/SerializationTuple.{cpp,h}`. Named and unnamed tuples serialize as parallel element columns, no wrapper. Nested is represented as a `Tuple` of `Array` columns with shared offsets.

### Map

`src/DataTypes/DataTypeMap.{cpp,h}`, `src/DataTypes/Serializations/SerializationMap.{cpp,h}`. Represented as `Array(Tuple(key, value))` on the wire.

### Decimal

`src/DataTypes/DataTypeDecimalBase.{cpp,h}`, `src/DataTypes/DataTypesDecimal.h`, `src/DataTypes/Serializations/SerializationDecimal.{cpp,h}`. Stored as fixed-width two's-complement integer (32/64/128/256-bit) with precision and scale carried in the type string.

### Enum8 / Enum16

`src/DataTypes/DataTypeEnum.{cpp,h}`, `src/DataTypes/EnumValues.{cpp,h}`, `src/DataTypes/Serializations/SerializationEnum.{cpp,h}`. Name-to-value map lives in the type string. Wire format is the underlying Int8/Int16.

### Date, Date32, DateTime, DateTime64

`src/DataTypes/DataTypeDate.{cpp,h}`, `src/DataTypes/DataTypeDate32.{cpp,h}`, `src/DataTypes/DataTypeDateTime.{cpp,h}`, `src/DataTypes/DataTypeDateTime64.{cpp,h}` and their `SerializationDate*` counterparts. `Date` is UInt16 days since 1970-01-01 (Unix epoch). `Date32` is Int32 days since 1970-01-01, extended range approximately 1900..2299. `DateTime` is UInt32 seconds since 1970-01-01 UTC. `DateTime64` is Int64 ticks at configurable precision 0..9. Timezone is part of the type string, not the wire payload. See also `src/DataTypes/TimezoneMixin.h` and `src/Common/DateLUTImpl.h` (`DATE_LUT_*` constants) for range and offset logic.

### UUID

`src/DataTypes/DataTypeUUID.{cpp,h}`, `src/DataTypes/Serializations/SerializationUUID.{cpp,h}`. Wire: 16 bytes, stored as UInt128. Byte order has historically tripped clients; confirm against the source before assuming.

### String and FixedString

`src/DataTypes/DataTypeString.{cpp,h}`, `src/DataTypes/DataTypeFixedString.{cpp,h}`, `src/DataTypes/Serializations/SerializationString.{cpp,h}`, `src/DataTypes/Serializations/SerializationFixedString.{cpp,h}`. String wire: VarUInt length prefix per value, then raw bytes. FixedString wire: fixed-width bytes, no length prefix.

### JSON / Object

`src/DataTypes/DataTypeObject.{cpp,h}`, `src/DataTypes/Serializations/SerializationObject.{cpp,h}`, `SerializationObjectDistinctPaths.{cpp,h}`, `SerializationObjectDynamicPath.{cpp,h}`, `SerializationObjectSharedData.{cpp,h}`, `SerializationJSON.{cpp,h}`. The newer JSON type uses a shared-data representation with dynamically tracked paths. Wire layout is not a plain string — confirm against the serialization code before implementing client read/write.

### Variant and Dynamic

`src/DataTypes/DataTypeVariant.{cpp,h}`, `src/DataTypes/DataTypeDynamic.{cpp,h}`, `src/DataTypes/Serializations/SerializationVariant.{cpp,h}`, `SerializationVariantElement.{cpp,h}`, `SerializationDynamic.{cpp,h}`, `SerializationDynamicElement.{cpp,h}`, `SerializationDynamicHelpers.{cpp,h}`. Variant: per-row discriminant byte selecting one of the variant type columns. Dynamic: self-describing variant with schema transmitted via `DataTypesBinaryEncoding`.

### IPv4 and IPv6

`src/DataTypes/DataTypeIPv4andIPv6.{cpp,h}`, `src/DataTypes/Serializations/SerializationIPv4andIPv6.{cpp,h}`. Wire: UInt32 for IPv4, 16 bytes for IPv6.

## Input/output formats

- **Format factory and settings**: `src/Formats/FormatFactory.{cpp,h}`, `src/Formats/FormatSettings.h`.
- **Format base classes**: `src/Processors/Formats/IInputFormat.h`, `IOutputFormat.h`, `IRowInputFormat.h`, `IRowOutputFormat.h`.
- **Native**: `src/Formats/NativeReader.cpp`, `src/Formats/NativeWriter.cpp` (binary block-oriented, also used as TCP payload).
- **JSONEachRow**: `src/Processors/Formats/Impl/JSONEachRowRowInputFormat.{cpp,h}`, `JSONEachRowRowOutputFormat.{cpp,h}`. Compact variant: `JSONCompactEachRowRowInputFormat.{cpp,h}`.
- **TabSeparated**: `src/Processors/Formats/Impl/TabSeparatedRowInputFormat.{cpp,h}`, `TabSeparatedRowOutputFormat.{cpp,h}`.
- **CSV**: `src/Processors/Formats/Impl/CSVRowInputFormat.{cpp,h}`, `CSVRowOutputFormat.{cpp,h}`.
- **Values**: `src/Processors/Formats/Impl/ValuesBlockInputFormat.{cpp,h}`, `ValuesRowOutputFormat.{cpp,h}`.

## Settings and context

- **Settings declarations**: `src/Core/Settings.h` declares every runtime setting via macros. `src/Core/SettingsFields.h` defines the underlying field types. `src/Core/BaseSettingsFwdMacros.h` wires the macro infrastructure.
- **Version history**: `src/Core/SettingsChangesHistory.h` tracks when settings were added, renamed, or had their default changed. First stop when a behavior changes across versions.
- **Query context**: `src/Interpreters/Context.h` holds `Settings` plus other per-query execution state. TCP and HTTP handlers apply client-provided setting overrides at query start.
- **Client setting delivery**: settings ride on the Query packet on TCP (see `src/Server/TCPHandler.cpp` and `src/Core/Protocol.h`) and on URL parameters via HTTP (`src/Server/HTTP/`).

## Errors and exceptions

- **Error codes**: `src/Common/ErrorCodes.h`, `src/Common/ErrorCodes.cpp`. Enumerates every error code and its name.
- **Exception class**: `src/Common/Exception.{cpp,h}`, with extended utilities in `src/Common/ExceptionExt.{cpp,h}`. Carries code, message, optional stack trace.
- **Wire transmission**: errors are sent as the Exception server packet (see `Protocol::Server::Exception` in `src/Core/Protocol.h`); emission lives in `src/Server/TCPHandler.cpp`.

## Compression

- **Codec interface**: `src/Compression/ICompressionCodec.h`.
- **Wire codecs**: `src/Compression/CompressionCodecLZ4.cpp`, `src/Compression/CompressionCodecZSTD.cpp` are the two you will see from the TCP client path. Specialized codecs (Delta, DoubleDelta, Gorilla, GCD, T64) exist alongside them but are used in on-disk/column-level compression rather than default wire transport.
- **Compressed buffers**: `src/Compression/CompressedReadBuffer.{cpp,h}`, `CompressedWriteBuffer.{cpp,h}`.
- **Factory**: `src/Compression/CompressionFactory.{cpp,h}`, `CompressionFactoryAdditions.cpp`.

## Server tests relevant to clients

Tests live under `tests/` in the checkout. Filenames embed the feature under test, so `rg`-style globs on `tests/queries/0_stateless/` are the fastest way in.

- **Type behavior**: `tests/queries/0_stateless/*<typename>*.sql`, for example `*nullable*.sql`, `*low_cardinality*.sql`, `*decimal*.sql`, `*datetime*.sql`, `*variant*.sql`, `*dynamic*.sql`.
- **JSON/Object type**: `tests/queries/0_stateless/*json*.sql`, `*object*.sql`.
- **Format behavior**: `*csv*.sql`, `*tsv*.sql`, `*values*.sql`, `*native*.sql`.
- **Protocol and compression**: `*protocol*.sql`, `*compression*.sql`.
- **Settings**: `*settings*.sql`.
- **C++ unit tests**: `src/DataTypes/tests/`, `src/Formats/tests/`, `src/Common/tests/` when present. Search with `fd -t d tests src/`.

Each test typically ships with a `.reference` file containing expected output. Those reference files are often the clearest spec of what the server guarantees.
