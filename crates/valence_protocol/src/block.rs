#![allow(clippy::all)] // TODO: block build script creates many warnings.

use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::io::Write;
use std::iter::FusedIterator;

use anyhow::Context;
use serde::{Deserialize, Deserializer};
use serde::de::Error;
use valence_protocol_macros::ident_str;

use crate::ident::Ident;
use crate::item::ItemKind;
use crate::var_int::VarInt;
use crate::{Decode, Encode, Result};

include!(concat!(env!("OUT_DIR"), "/block.rs"));

impl fmt::Debug for BlockState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt_block_state(*self, f)
    }
}

impl Display for BlockState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt_block_state(*self, f)
    }
}

fn fmt_block_state(bs: BlockState, f: &mut fmt::Formatter) -> fmt::Result {
    let kind = bs.to_kind();

    write!(f, "{}", kind.to_str())?;

    let props = kind.props();

    if !props.is_empty() {
        let mut list = f.debug_list();
        for &p in kind.props() {
            struct KeyVal<'a>(&'a str, &'a str);

            impl<'a> fmt::Debug for KeyVal<'a> {
                fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                    write!(f, "{}={}", self.0, self.1)
                }
            }

            list.entry(&KeyVal(p.to_str(), bs.get(p).unwrap().to_str()));
        }
        list.finish()
    } else {
        Ok(())
    }
}

impl Encode for BlockState {
    fn encode(&self, w: impl Write) -> Result<()> {
        VarInt(self.to_raw() as i32).encode(w)
    }
}

impl Decode<'_> for BlockState {
    fn decode(r: &mut &[u8]) -> Result<Self> {
        let id = VarInt::decode(r)?.0;
        let errmsg = "invalid block state ID";

        BlockState::from_raw(id.try_into().context(errmsg)?).context(errmsg)
    }
}

impl Encode for BlockKind {
    fn encode(&self, w: impl Write) -> Result<()> {
        VarInt(self.to_raw() as i32).encode(w)
    }
}

impl Decode<'_> for BlockKind {
    fn decode(r: &mut &[u8]) -> Result<Self> {
        let id = VarInt::decode(r)?.0;
        let errmsg = "invalid block kind ID";

        BlockKind::from_raw(id.try_into().context(errmsg)?).context(errmsg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_set_consistency() {
        for kind in BlockKind::ALL {
            let block = kind.to_state();

            for &prop in kind.props() {
                let new_block = block.set(prop, block.get(prop).unwrap());
                assert_eq!(new_block, block);
            }
        }
    }

    #[test]
    fn blockstate_to_wall() {
        assert_eq!(BlockState::STONE.wall_block_id(), None);
        assert_eq!(
            BlockState::OAK_SIGN.wall_block_id(),
            Some(BlockState::OAK_WALL_SIGN)
        );
        assert_eq!(
            BlockState::GREEN_BANNER.wall_block_id(),
            Some(BlockState::GREEN_WALL_BANNER)
        );
        assert_ne!(
            BlockState::GREEN_BANNER.wall_block_id(),
            Some(BlockState::GREEN_BANNER)
        );
    }
}

// Deserialize BlockStates from minecraft's datapacks using serde
impl<'de> Deserialize<'de> for BlockState {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error> where D: Deserializer<'de> {
        #[derive(Deserialize)]
        struct Raw {
            #[serde(rename = "Name")]
            name: Ident<String>,
            #[serde(rename = "Properties", default)]
            properties: HashMap<String, String>,
        }

        impl TryFrom<Raw> for BlockState {
            type Error = String;

            fn try_from(value: Raw) -> std::result::Result<Self, Self::Error> {
                if value.name.namespace() != "minecraft" {
                    return Err("only blocks from the minecraft namespace can be deserialized".to_string());
                }

                let kind = match BlockKind::from_str(value.name.path()) {
                    None => return Err(format!("unknown block kind \"{}\"", value.name)),
                    Some(kind) => kind,
                };

                let mut state = BlockState::from_kind(kind);
                for (key, value) in value.properties {
                    let name = match PropName::from_str(&key) {
                        None => return Err(format!("unknown property \"{key}\"")),
                        Some(name) => name
                    };
                    let value = match PropValue::from_str(&value) {
                        None => return Err(format!("unable to parse property value \"{value}\" for \"{key}\"")),
                        Some(value) => value
                    };

                    state = state.set(name, value);
                }

                Ok(state)
            }
        }

        Raw::deserialize(deserializer)?.try_into().map_err(|e| D::Error::custom(e))
    }
}