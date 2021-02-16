// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use ligature::{Attribute, Dataset, LigatureError};

pub const DATASET_PREFIX: u8 = 0;
pub const ENTITY_ID_COUNTER_KEY: u8 = 1;
pub const ATTRIBUTE_ID_COUNTER_KEY: u8 = 2;
pub const ATTRIBUTE_NAME_TO_ID_PREFIX: u8 = 3;
pub const ATTRIBUTE_ID_TO_NAME_PREFIX: u8 = 4;
pub const EAVC_PREFIX: u8 = 5;
pub const EVAC_PREFIX: u8 = 6;
pub const AEVC_PREFIX: u8 = 7;
pub const AVEC_PREFIX: u8 = 8;
pub const VEAC_PREFIX: u8 = 9;
pub const VAEC_PREFIX: u8 = 10;
pub const CEAV_PREFIX: u8 = 11;
pub const STRING_LITERAL_ID_COUNTER_KEY: u8 = 12;
pub const STRING_LITERAL_VALUE_TO_ID_PREFIX: u8 = 13;
pub const STRING_LITERAL_ID_TO_VALUE_PREFIX: u8 = 14;

pub const ENTITY_VALUE_PREFIX: u8 = 0;
pub const STRING_VALUE_PREFIX: u8 = 1;
pub const INTEGER_VALUE_PREFIX: u8 = 2;
pub const FLOAT_VALUE_PREFIX: u8 = 3;

pub struct StatementIDSet {
    pub entity_id: u64,
    pub attribute_id: u64,
    pub value_prefix: u8,
    pub value_body: Vec<u8>, //value body can hold either an encoded i64 f64 or in the case of a String literal u64
    pub context_id: u64,
}

/// Prepends the given prefix to the given vector.
pub fn prepend(prefix: u8, vector: Vec<u8>) -> Vec<u8> {
    let mut res = vec![prefix];
    res.extend(vector);
    res
}

/// Removes the first value from a Vec<u8> and checks that it's the expected value.
pub fn chomp_assert(prefix: u8, vector: Vec<u8>) -> Result<Vec<u8>, LigatureError> {
    let mut vector_clone = vector;
    let vec_prefix = vector_clone.remove(0);
    if prefix == vec_prefix {
        Ok(vector_clone)
    } else {
        Err(LigatureError(format!(
            "Expected prefix {}, actual prefix {}.",
            prefix, vec_prefix
        )))
    }
}

/// Encodes a Dataset's Name.
pub fn encode_dataset(dataset: &Dataset) -> Vec<u8> {
    dataset.name().as_bytes().to_vec()
}

/// Encodes a str prefix used to match Dataset names.
/// This is needed since a Dataset prefix might not be a valid Dataset name.
pub fn encode_dataset_match(dataset_match: &str) -> Vec<u8> {
    dataset_match.as_bytes().to_vec()
}

/// Decodes a Dataset's name and returns a Dataset.
pub fn decode_dataset(dataset: Vec<u8>) -> Result<Dataset, LigatureError> {
    let name = std::str::from_utf8(&dataset)
        .map_err(|_| LigatureError("Error checking for Dataset".to_string()))?;
    Dataset::new(name)
}

/// Encodes the value stored for an id.
pub fn encode_id(id: u64) -> Vec<u8> {
    id.to_be_bytes().to_vec()
}

/// Decodes the value stored for an id.
pub fn decode_id(id: Vec<u8>) -> Result<u64, LigatureError> {
    if id.len() == 8 {
        Ok(u64::from_be_bytes([
            id[0], id[1], id[2], id[3], id[4], id[5], id[6], id[7],
        ]))
    } else {
        Err(LigatureError(format!("Could not convert {:?} to u64", id)))
    }
}

pub fn decode_integer(id: Vec<u8>) -> Result<i64, LigatureError> {
    if id.len() == 8 {
        Ok(i64::from_be_bytes([
            id[0], id[1], id[2], id[3], id[4], id[5], id[6], id[7],
        ]))
    } else {
        Err(LigatureError(format!("Could not convert {:?} to i64", id)))
    }
}

pub fn decode_float(id: Vec<u8>) -> Result<f64, LigatureError> {
    if id.len() == 8 {
        Ok(f64::from_be_bytes([
            id[0], id[1], id[2], id[3], id[4], id[5], id[6], id[7],
        ]))
    } else {
        Err(LigatureError(format!("Could not convert {:?} to f64", id)))
    }
}

pub fn encode_attribute(attribute: &Attribute) -> Vec<u8> {
    attribute.name().as_bytes().to_vec()
}

pub fn decode_attribute(attribute: Vec<u8>) -> Result<Attribute, LigatureError> {
    let attribute_name = std::str::from_utf8(&attribute).map_err(|_| {
        LigatureError(format!(
            "Error converting encoded Attribute to Attribute - {:?}",
            attribute
        ))
    })?;
    Attribute::new(attribute_name)
}

/// Encodes a String Literal and prefixes it with STRING_VALUE_PREFIX.
pub fn encode_string_literal(string: &str) -> Vec<u8> {
    string.as_bytes().to_vec()
}

fn flatten(v: Vec<&Vec<u8>>) -> Vec<u8> {
    v.into_iter().cloned().flatten().collect()
}

/// Accepts a StatementIDs struct and returns a Vec with 7 entries, one for each of the different permutations.
pub fn encode_statement_permutations(statement_ids: &StatementIDSet) -> Vec<Vec<u8>> {
    let entity = encode_id(statement_ids.entity_id);
    let attribute = encode_id(statement_ids.attribute_id);
    let value = prepend(statement_ids.value_prefix, statement_ids.value_body.clone());
    let context = encode_id(statement_ids.context_id);

    let eavc = flatten(vec![
        &vec![EAVC_PREFIX],
        &entity,
        &attribute,
        &value,
        &context,
    ]);
    let evac = flatten(vec![
        &vec![EVAC_PREFIX],
        &entity,
        &value,
        &attribute,
        &context,
    ]);
    let aevc = flatten(vec![
        &vec![AEVC_PREFIX],
        &attribute,
        &entity,
        &value,
        &context,
    ]);
    let avec = flatten(vec![
        &vec![AVEC_PREFIX],
        &attribute,
        &value,
        &entity,
        &context,
    ]);
    let veac = flatten(vec![
        &vec![VEAC_PREFIX],
        &value,
        &entity,
        &attribute,
        &context,
    ]);
    let vaec = flatten(vec![
        &vec![VAEC_PREFIX],
        &value,
        &attribute,
        &entity,
        &context,
    ]);
    let ceav = flatten(vec![
        &vec![CEAV_PREFIX],
        &context,
        &entity,
        &attribute,
        &value,
    ]);

    vec![eavc, evac, aevc, avec, veac, vaec, ceav]
}

/// Accepts a permutation prefixed encoded statement and uses that to break up the parts into a StatementIDSet.
pub fn decode_statement_permutation(
    mut statement: Vec<u8>,
) -> Result<StatementIDSet, LigatureError> {
    let prefix = chomp_u8(&mut statement)?;
    match prefix {
        EAVC_PREFIX => decode_eavc(&mut statement),
        EVAC_PREFIX => decode_evac(&mut statement),
        AEVC_PREFIX => decode_aevc(&mut statement),
        AVEC_PREFIX => decode_avec(&mut statement),
        VEAC_PREFIX => decode_veac(&mut statement),
        VAEC_PREFIX => decode_vaec(&mut statement),
        CEAV_PREFIX => decode_ceav(&mut statement),
        _ => Err(LigatureError(format!(
            "Error decoding Statement -- {:?}",
            statement
        ))),
    }
}

/// Removes a u8 from the beginning of a Vec<u8>.
fn chomp_u8(vec: &mut Vec<u8>) -> Result<u8, LigatureError> {
    if vec.is_empty() {
        Err(LigatureError("Vector is empty.".to_string()))
    } else {
        Ok(vec.remove(0))
    }
}

/// Removes a u64 from the beginning of a Vec<u8>.
fn chomp_u64(vec: &mut Vec<u8>) -> Result<u64, LigatureError> {
    if vec.len() < 8 {
        Err(LigatureError(
            "Vector is not large enough to chomp u64.".to_string(),
        ))
    } else {
        Ok(u64::from_be_bytes([
            vec.remove(0),
            vec.remove(0),
            vec.remove(0),
            vec.remove(0),
            vec.remove(0),
            vec.remove(0),
            vec.remove(0),
            vec.remove(0),
        ]))
    }
}

fn chomp_vec_u8(vec: &mut Vec<u8>) -> Result<Vec<u8>, LigatureError> {
    if vec.len() < 8 {
        Err(LigatureError(
            "Vector is not large enough to chomp Vec<u8> len 8.".to_string(),
        ))
    } else {
        Ok(vec![
            vec.remove(0),
            vec.remove(0),
            vec.remove(0),
            vec.remove(0),
            vec.remove(0),
            vec.remove(0),
            vec.remove(0),
            vec.remove(0),
        ])
    }
}

fn decode_eavc(statement: &mut Vec<u8>) -> Result<StatementIDSet, LigatureError> {
    Ok(StatementIDSet {
        entity_id: chomp_u64(statement)?,
        attribute_id: chomp_u64(statement)?,
        value_prefix: chomp_u8(statement)?,
        value_body: chomp_vec_u8(statement)?,
        context_id: chomp_u64(statement)?,
    })
}

fn decode_evac(statement: &mut Vec<u8>) -> Result<StatementIDSet, LigatureError> {
    Ok(StatementIDSet {
        entity_id: chomp_u64(statement)?,
        value_prefix: chomp_u8(statement)?,
        value_body: chomp_vec_u8(statement)?,
        attribute_id: chomp_u64(statement)?,
        context_id: chomp_u64(statement)?,
    })
}

fn decode_aevc(statement: &mut Vec<u8>) -> Result<StatementIDSet, LigatureError> {
    Ok(StatementIDSet {
        attribute_id: chomp_u64(statement)?,
        entity_id: chomp_u64(statement)?,
        value_prefix: chomp_u8(statement)?,
        value_body: chomp_vec_u8(statement)?,
        context_id: chomp_u64(statement)?,
    })
}

fn decode_avec(statement: &mut Vec<u8>) -> Result<StatementIDSet, LigatureError> {
    Ok(StatementIDSet {
        attribute_id: chomp_u64(statement)?,
        value_prefix: chomp_u8(statement)?,
        value_body: chomp_vec_u8(statement)?,
        entity_id: chomp_u64(statement)?,
        context_id: chomp_u64(statement)?,
    })
}

fn decode_veac(statement: &mut Vec<u8>) -> Result<StatementIDSet, LigatureError> {
    Ok(StatementIDSet {
        value_prefix: chomp_u8(statement)?,
        value_body: chomp_vec_u8(statement)?,
        entity_id: chomp_u64(statement)?,
        attribute_id: chomp_u64(statement)?,
        context_id: chomp_u64(statement)?,
    })
}

fn decode_vaec(statement: &mut Vec<u8>) -> Result<StatementIDSet, LigatureError> {
    Ok(StatementIDSet {
        value_prefix: chomp_u8(statement)?,
        value_body: chomp_vec_u8(statement)?,
        attribute_id: chomp_u64(statement)?,
        entity_id: chomp_u64(statement)?,
        context_id: chomp_u64(statement)?,
    })
}

fn decode_ceav(statement: &mut Vec<u8>) -> Result<StatementIDSet, LigatureError> {
    Ok(StatementIDSet {
        context_id: chomp_u64(statement)?,
        entity_id: chomp_u64(statement)?,
        attribute_id: chomp_u64(statement)?,
        value_prefix: chomp_u8(statement)?,
        value_body: chomp_vec_u8(statement)?,
    })
}
