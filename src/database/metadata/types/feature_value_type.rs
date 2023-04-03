#[derive(sqlx::Type, Default, PartialEq, Debug, Clone)]
pub enum FeatureValueType {
    #[default]
    StringType,
    Int64,
    Float64,
    Bool,
    Time,
    Bytes,
    Invalid,
}

impl std::convert::From<String> for FeatureValueType {
    fn from(v: String) -> Self {
        match v.as_str() {
            "string" => FeatureValueType::StringType,
            "int64" => FeatureValueType::Int64,
            "float64" => FeatureValueType::Float64,
            "bool" => FeatureValueType::Bool,
            "time" => FeatureValueType::Time,
            "bytes" => FeatureValueType::Bytes,
            _ => FeatureValueType::Invalid,
        }
    }
}
