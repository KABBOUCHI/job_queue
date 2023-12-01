use sqlx::any::AnyTypeInfo;
use sqlx::decode::Decode;
use sqlx::postgres::any::{AnyTypeInfoKind, AnyValueKind};
use sqlx::{database::HasValueRef, error::BoxDynError, Any};
use sqlx::{Type, ValueRef};

#[derive(Debug)]
pub struct JsonValue(pub serde_json::Value);

#[derive(Debug, sqlx::FromRow)]
pub struct Task {
    pub id: i64,
    pub uuid: String,
    pub payload: JsonValue,
    pub attempts: i16,
    // available_at: i64,
    // created_at: i64,
}

#[derive(Debug, sqlx::FromRow)]
pub struct FailedJob {
    pub id: i64,
    pub uuid: String,
    pub queue: String,
    pub payload: JsonValue,
    // available_at: i64,
    // created_at: i64,
}

impl Type<Any> for JsonValue {
    fn type_info() -> AnyTypeInfo {
        AnyTypeInfo {
            kind: AnyTypeInfoKind::Blob,
        }
    }

    fn compatible(ty: &AnyTypeInfo) -> bool {
        matches!(ty.kind, AnyTypeInfoKind::Blob | AnyTypeInfoKind::Text)
    }
}

impl<'r> Decode<'r, Any> for JsonValue {
    fn decode(value: <Any as HasValueRef<'r>>::ValueRef) -> Result<Self, BoxDynError> {
        let v = ValueRef::to_owned(&value);

        match v.kind {
            AnyValueKind::Blob(s) => Ok(JsonValue(serde_json::from_slice(&s)?)),
            AnyValueKind::Text(s) => Ok(JsonValue(serde_json::from_str(&s)?)),
            _ => Err("invalid type".into()),
        }
    }
}
