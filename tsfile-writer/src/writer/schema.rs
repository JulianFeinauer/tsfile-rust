//! Contains the classes for defining the Schema of a TsFile, i.e. which Devices / Snesors
//! it contains and their datatype / encoding / compression
use crate::writer::{
    CompressionType, MeasurementGroup, MeasurementSchema, Schema, TSDataType, TSEncoding,
};
use std::collections::HashMap;

pub struct TsFileSchemaBuilder<'a> {
    measurement_groups_map: HashMap<&'a str, MeasurementGroup<'a>>,
}

impl<'a> TsFileSchemaBuilder<'a> {
    pub fn new() -> TsFileSchemaBuilder<'a> {
        TsFileSchemaBuilder {
            measurement_groups_map: HashMap::new(),
        }
    }

    pub fn add(
        &mut self,
        device: &'a str,
        schema: MeasurementGroup<'a>,
    ) -> &mut TsFileSchemaBuilder<'a> {
        self.measurement_groups_map.insert(device, schema);
        self
    }

    pub fn build(&mut self) -> Schema<'a> {
        // Copy the content
        let mut measurement_groups: HashMap<&str, MeasurementGroup> = HashMap::new();
        measurement_groups.clear();
        for (s, mg) in self.measurement_groups_map.iter_mut() {
            measurement_groups.insert(s, mg.clone());
        }
        Schema { measurement_groups }
    }
}

impl<'a> Default for TsFileSchemaBuilder<'a> {
    fn default() -> Self {
        Self::new()
    }
}

pub struct DeviceBuilder<'a> {
    measurement_groups_map: HashMap<&'a str, MeasurementSchema>,
}

impl<'a> DeviceBuilder<'a> {
    pub fn new() -> DeviceBuilder<'a> {
        DeviceBuilder {
            measurement_groups_map: HashMap::new(),
        }
    }

    pub fn add(
        &mut self,
        measurement: &'a str,
        data_type: TSDataType,
        encoding: TSEncoding,
        compression: CompressionType,
    ) -> &mut DeviceBuilder<'a> {
        self.measurement_groups_map.insert(
            measurement,
            MeasurementSchema {
                data_type,
                compression,
                encoding,
            },
        );
        self
    }

    pub fn build(&mut self) -> MeasurementGroup<'a> {
        assert!(!self.measurement_groups_map.is_empty());
        // Copy the content
        let mut measurement_schemas: HashMap<&'a str, MeasurementSchema> = HashMap::new();
        measurement_schemas.clear();
        for (s, ms) in self.measurement_groups_map.iter_mut() {
            measurement_schemas.insert(s, ms.clone());
        }
        MeasurementGroup {
            measurement_schemas,
        }
    }
}

impl<'a> Default for DeviceBuilder<'a> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod test {
    use crate::schema::{DeviceBuilder, TsFileSchemaBuilder};
    use crate::{CompressionType, TSDataType, TSEncoding};

    #[test]
    fn use_fluent_builder() {
        let schema = TsFileSchemaBuilder::new()
            .add(
                "d1",
                DeviceBuilder::new()
                    .add(
                        "s1",
                        TSDataType::INT32,
                        TSEncoding::PLAIN,
                        CompressionType::UNCOMPRESSED,
                    )
                    .add(
                        "s2",
                        TSDataType::INT32,
                        TSEncoding::PLAIN,
                        CompressionType::UNCOMPRESSED,
                    )
                    .build(),
            )
            .build();

        assert_eq!(schema.measurement_groups.len(), 1);
        assert_eq!(
            schema
                .measurement_groups
                .get("d1")
                .unwrap()
                .measurement_schemas
                .len(),
            2
        );
    }
}
