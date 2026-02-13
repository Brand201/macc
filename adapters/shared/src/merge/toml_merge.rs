use toml::Value;

pub fn deep_merge(base: &mut Value, overlay: &Value) {
    match (base, overlay) {
        (Value::Table(base_map), Value::Table(overlay_map)) => {
            for (k, v) in overlay_map {
                match base_map.get_mut(k) {
                    Some(existing) => deep_merge(existing, v),
                    None => {
                        base_map.insert(k.clone(), v.clone());
                    }
                }
            }
        }
        (Value::Array(base_arr), Value::Array(overlay_arr)) => {
            for item in overlay_arr {
                if !base_arr.contains(item) {
                    base_arr.push(item.clone());
                }
            }
        }
        (base_slot, overlay_value) => {
            *base_slot = overlay_value.clone();
        }
    }
}
