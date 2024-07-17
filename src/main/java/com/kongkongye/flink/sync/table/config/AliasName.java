package com.kongkongye.flink.sync.table.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AliasName implements Serializable {
    public String name;
    public String alias;

    public static AliasName of(String name) {
        if (name.contains(" ")) {
            String[] split = name.split(" ");
            return new AliasName(split[0], split[1]);
        }else {
            return new AliasName(name, name);
        }
    }
}
