//! CPU profile rows and the OTLP Profiles v1development wire subset.
//!
//! Field numbers follow opentelemetry-proto v1.10.0 (Apache-2.0).
//! Unknown protobuf fields are ignored. Only CPU/count and CPU/nanoseconds
//! profiles are accepted; this is not a receiver for arbitrary profile types.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, clickhouse::Row)]
pub struct ProfileRow {
    pub tenant_id: String,
    pub timestamp: i64,
    pub service_name: String,
    pub service_version: String,
    pub pod: String,
    pub profile_type: String,
    pub profile_id: String,
    pub sample_index: u64,
    pub value_index: u64,
    pub duration_nano: u64,
    pub cpu_nanoseconds: u64,
    /// Root first. LowCardinality strings share frame names on disk.
    pub frames: Vec<String>,
    pub trace_id: String,
    pub span_id: String,
}

pub mod wire {
    use opentelemetry_proto::tonic::resource::v1::Resource;
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct ExportProfilesServiceRequest {
        #[prost(message, repeated, tag = "1")]
        pub resource_profiles: Vec<ResourceProfiles>,
        #[prost(message, optional, tag = "2")]
        pub dictionary: Option<Dictionary>,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct ResourceProfiles {
        #[prost(message, optional, tag = "1")]
        pub resource: Option<Resource>,
        #[prost(message, repeated, tag = "2")]
        pub scope_profiles: Vec<ScopeProfiles>,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct ScopeProfiles {
        #[prost(message, repeated, tag = "2")]
        pub profiles: Vec<Profile>,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Profile {
        #[prost(message, optional, tag = "1")]
        pub sample_type: Option<ValueType>,
        #[prost(message, repeated, tag = "2")]
        pub samples: Vec<Sample>,
        #[prost(fixed64, tag = "3")]
        pub time_unix_nano: u64,
        #[prost(uint64, tag = "4")]
        pub duration_nano: u64,
        #[prost(message, optional, tag = "5")]
        pub period_type: Option<ValueType>,
        #[prost(int64, tag = "6")]
        pub period: i64,
        #[prost(bytes = "vec", tag = "7")]
        pub profile_id: Vec<u8>,
        #[prost(string, tag = "9")]
        pub original_payload_format: String,
        #[prost(bytes = "vec", tag = "10")]
        pub original_payload: Vec<u8>,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct ValueType {
        #[prost(int32, tag = "1")]
        pub type_strindex: i32,
        #[prost(int32, tag = "2")]
        pub unit_strindex: i32,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Sample {
        #[prost(int32, tag = "1")]
        pub stack_index: i32,
        #[prost(int64, repeated, tag = "4")]
        pub values: Vec<i64>,
        #[prost(int32, repeated, tag = "2")]
        pub attribute_indices: Vec<i32>,
        #[prost(int32, tag = "3")]
        pub link_index: i32,
        #[prost(fixed64, repeated, tag = "5")]
        pub timestamps_unix_nano: Vec<u64>,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Dictionary {
        #[prost(message, repeated, tag = "6")]
        pub attribute_table: Vec<ProfileAttribute>,
        #[prost(message, repeated, tag = "1")]
        pub mapping_table: Vec<Mapping>,
        #[prost(message, repeated, tag = "2")]
        pub location_table: Vec<Location>,
        #[prost(message, repeated, tag = "3")]
        pub function_table: Vec<Function>,
        #[prost(message, repeated, tag = "4")]
        pub link_table: Vec<Link>,
        #[prost(string, repeated, tag = "5")]
        pub string_table: Vec<String>,
        #[prost(message, repeated, tag = "7")]
        pub stack_table: Vec<Stack>,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Mapping {
        #[prost(uint64, tag = "1")]
        pub memory_start: u64,
        #[prost(int32, tag = "4")]
        pub filename_strindex: i32,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Stack {
        #[prost(int32, repeated, tag = "1")]
        pub location_indices: Vec<i32>,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Location {
        #[prost(int32, tag = "1")]
        pub mapping_index: i32,
        #[prost(uint64, tag = "2")]
        pub address: u64,
        #[prost(message, repeated, tag = "3")]
        pub lines: Vec<Line>,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Line {
        #[prost(int32, tag = "1")]
        pub function_index: i32,
        #[prost(int64, tag = "2")]
        pub line: i64,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Function {
        #[prost(int32, tag = "1")]
        pub name_strindex: i32,
        #[prost(int32, tag = "2")]
        pub system_name_strindex: i32,
        #[prost(int32, tag = "3")]
        pub filename_strindex: i32,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Link {
        #[prost(bytes = "vec", tag = "1")]
        pub trace_id: Vec<u8>,
        #[prost(bytes = "vec", tag = "2")]
        pub span_id: Vec<u8>,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct ProfileAttribute {
        #[prost(int32, tag = "1")]
        pub key_strindex: i32,
        #[prost(message, optional, tag = "2")]
        pub value: Option<opentelemetry_proto::tonic::common::v1::AnyValue>,
    }
}
