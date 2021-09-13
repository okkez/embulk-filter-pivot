# Pivot filter plugin for Embulk

Embulk filter plugin to pivot columns to rows.

## Overview

* **Plugin type**: filter

## Configuration

- **common_columns**: description (Array, default: `[]`)
- **key_config**: description (ColumnConfig)
  - **name**: name of the column (default: `"key"`)
  - **type**: type of the column (default: `"string"`)
- **value_config**: description (ColumnConfig, default: `null`)
    - **name**: name of the column (default: `"key"`)
    - **type**: type of the column (default: `"string"`)

## Example

```yaml
filters:
  - type: pivot
    common_columns:
      - user_id
      - project
    key_config: {name: "my_key", type: "string"}
    value_config: {name: "my_value", type: "string"}
```

| user_id:string | project:string | city:string |  phone:string | gender:string |
| ---            | ---            | ---         |           --- | ---           |
| user-100       | test-project   | Tokyo       | 080-xxxx-xxx1 | male          |
| user-101       | test-project   | Osaka       | 080-xxxx-xxx2 | male          |
| user-201       | test-project   | Kyoto       | 080-xxxx-xxx3 | female        |
| user-301       | test-project   | Nara        | 080-xxxx-xxx4 | other         |

will convert to followings:

| user_id:string | project:string | my_key:string | my_value:string |
| ---            | ---            | ---           | ---             |
| user-100       | test-project   | city          | Tokyo           |
| user-100       | test-project   | phone         | 080-xxxx-xxx1   |
| user-100       | test-project   | gender        | male            |
| user-101       | test-project   | city          | Osaka           |
| user-101       | test-project   | phone         | 080-xxxx-xxx2   |
| user-101       | test-project   | gender        | male            |
| user-201       | test-project   | city          | Kyoto           |
| user-201       | test-project   | phone         | 080-xxxx-xxx3   |
| user-201       | test-project   | gender        | female          |
| user-301       | test-project   | city          | Nara            |
| user-301       | test-project   | phone         | 080-xxxx-xxx4  |
| user-301       | test-project   | gender        | female          |

We should use the same types except for `common_columns` and key column specified by `key_config`.

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
