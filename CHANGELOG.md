# Changelog

## [1.1.2](https://github.com/rudderlabs/sqlconnect-go/compare/v1.1.1...v1.1.2) (2024-04-09)


### Bug Fixes

* **snowflake:** audiences stopped supporting views ([#43](https://github.com/rudderlabs/sqlconnect-go/issues/43)) ([3c5b7e5](https://github.com/rudderlabs/sqlconnect-go/commit/3c5b7e56d87d4ed1c5c70d5d0b27cc29028cc413))


### Miscellaneous

* **deps:** bump github.com/aws/aws-sdk-go-v2/credentials from 1.16.2 to 1.17.10 ([#37](https://github.com/rudderlabs/sqlconnect-go/issues/37)) ([19f7873](https://github.com/rudderlabs/sqlconnect-go/commit/19f78739bb1d8f4d866774280bfa02fbbfd36c3a))
* **deps:** bump github.com/aws/aws-sdk-go-v2/credentials from 1.17.10 to 1.17.11 ([#42](https://github.com/rudderlabs/sqlconnect-go/issues/42)) ([f84cb71](https://github.com/rudderlabs/sqlconnect-go/commit/f84cb7196d5f7109eb2520ba62fe0c6751baf40f))
* **deps:** bump github.com/aws/aws-sdk-go-v2/service/redshiftdata from 1.25.2 to 1.25.4 ([#36](https://github.com/rudderlabs/sqlconnect-go/issues/36)) ([2fe4a4d](https://github.com/rudderlabs/sqlconnect-go/commit/2fe4a4d86a1d8208bde38539ae584e6481d75592))
* **deps:** bump github.com/rudderlabs/rudder-go-kit from 0.23.3 to 0.24.0 ([#40](https://github.com/rudderlabs/sqlconnect-go/issues/40)) ([cc02180](https://github.com/rudderlabs/sqlconnect-go/commit/cc02180ad1a8f9da3edb00311d6f74bcb0e0bc20))

## [1.1.1](https://github.com/rudderlabs/sqlconnect-go/compare/v1.1.0...v1.1.1) (2024-04-01)


### Bug Fixes

* bigquery driver not honouring context while querying ([#34](https://github.com/rudderlabs/sqlconnect-go/issues/34)) ([653a194](https://github.com/rudderlabs/sqlconnect-go/commit/653a194ab0b5d1084ae80975b088c5ad73fb609e))


### Miscellaneous

* **databricks:** use full_data_type instead of data_type when listing columns ([#30](https://github.com/rudderlabs/sqlconnect-go/issues/30)) ([84b40fb](https://github.com/rudderlabs/sqlconnect-go/commit/84b40fbca57bd09a881d976b30381cfda3ad9f96))
* handle date and time data type mappings for postgres and redshift ([#32](https://github.com/rudderlabs/sqlconnect-go/issues/32)) ([3bab2e1](https://github.com/rudderlabs/sqlconnect-go/commit/3bab2e1031234ea342d73b940e33dac548031f01))

## [1.1.0](https://github.com/rudderlabs/sqlconnect-go/compare/v1.0.1...v1.1.0) (2024-03-26)


### Features

* add ssh tunneling support ([#26](https://github.com/rudderlabs/sqlconnect-go/issues/26)) ([7f8a686](https://github.com/rudderlabs/sqlconnect-go/commit/7f8a68686f4eae1f5bd5aeacd70da006f85808e6))
* introduce util.SplitStatements ([#14](https://github.com/rudderlabs/sqlconnect-go/issues/14)) ([4039fcc](https://github.com/rudderlabs/sqlconnect-go/commit/4039fccd94bce83ada025064e5de0cfe9363336e))
* redshift data driver ([#18](https://github.com/rudderlabs/sqlconnect-go/issues/18)) ([6555d97](https://github.com/rudderlabs/sqlconnect-go/commit/6555d9767cdfdfc112ca9c0ae7c0cd9c6abc783c))


### Miscellaneous

* **deps:** bump actions/setup-go from 3 to 5 ([#24](https://github.com/rudderlabs/sqlconnect-go/issues/24)) ([8bd2bd2](https://github.com/rudderlabs/sqlconnect-go/commit/8bd2bd2095096417ce1c38bd0aa9d41210e99712))
* **deps:** bump actions/stale from 5 to 9 ([#25](https://github.com/rudderlabs/sqlconnect-go/issues/25)) ([034f881](https://github.com/rudderlabs/sqlconnect-go/commit/034f881755645b6f627d6a7cce6fb4fc338508d9))
* **deps:** bump amannn/action-semantic-pull-request from 4 to 5 ([#23](https://github.com/rudderlabs/sqlconnect-go/issues/23)) ([b00a62e](https://github.com/rudderlabs/sqlconnect-go/commit/b00a62e053a58234cb2eee0a357dd50d51713888))
* **deps:** bump github.com/docker/docker ([#27](https://github.com/rudderlabs/sqlconnect-go/issues/27)) ([87f2a09](https://github.com/rudderlabs/sqlconnect-go/commit/87f2a09112ce255963291aab347c2ed81a562b8f))
* **deps:** bump github.com/rudderlabs/rudder-go-kit ([#22](https://github.com/rudderlabs/sqlconnect-go/issues/22)) ([00c6f0f](https://github.com/rudderlabs/sqlconnect-go/commit/00c6f0fce1c2d8cedfc252f4927c1ab5308b75a5))
* **deps:** bump github.com/rudderlabs/rudder-go-kit from 0.23.2 to 0.23.3 ([#29](https://github.com/rudderlabs/sqlconnect-go/issues/29)) ([8bbc3eb](https://github.com/rudderlabs/sqlconnect-go/commit/8bbc3eb6dbd021d54d0c8afd751a80a04ee6895b))
* **deps:** bump google.golang.org/api from 0.169.0 to 0.170.0 ([#21](https://github.com/rudderlabs/sqlconnect-go/issues/21)) ([c49e4e3](https://github.com/rudderlabs/sqlconnect-go/commit/c49e4e34b975c7d06ab2015a20c157cd0ce03879))
* **deps:** bump google.golang.org/api from 0.170.0 to 0.171.0 ([#28](https://github.com/rudderlabs/sqlconnect-go/issues/28)) ([0f7db9b](https://github.com/rudderlabs/sqlconnect-go/commit/0f7db9b3e4d210a38d045888c0ed9eb57a99275a))
* **deps:** bump google.golang.org/protobuf from 1.32.0 to 1.33.0 ([#15](https://github.com/rudderlabs/sqlconnect-go/issues/15)) ([f20b6c5](https://github.com/rudderlabs/sqlconnect-go/commit/f20b6c5727976d6a5727b86c69c6d0e720cf8ed8))
* respect catalog parameter in DB.ListColumns ([#16](https://github.com/rudderlabs/sqlconnect-go/issues/16)) ([11bf6b2](https://github.com/rudderlabs/sqlconnect-go/commit/11bf6b2efc9e566ecaba4780b4a47282ae7f5cb7))

## [1.0.1](https://github.com/rudderlabs/sqlconnect-go/compare/v1.0.0...v1.0.1) (2024-03-12)


### Miscellaneous

* **deps:** bump github.com/rudderlabs/rudder-go-kit ([#9](https://github.com/rudderlabs/sqlconnect-go/issues/9)) ([1f27788](https://github.com/rudderlabs/sqlconnect-go/commit/1f27788c4da796051c0b10e3a1ad203d2c4c8cd8))
* **deps:** bump github.com/stretchr/testify from 1.8.4 to 1.9.0 ([#12](https://github.com/rudderlabs/sqlconnect-go/issues/12)) ([45319cc](https://github.com/rudderlabs/sqlconnect-go/commit/45319cc58b62c52c9c58757794f772abc6cd3abb))
* **deps:** bump google.golang.org/api from 0.166.0 to 0.169.0 ([#10](https://github.com/rudderlabs/sqlconnect-go/issues/10)) ([e4aa239](https://github.com/rudderlabs/sqlconnect-go/commit/e4aa23987e6b697517238303a6e40be5ffac3f52))
* **deps:** bump the go_modules group group with 1 update ([#8](https://github.com/rudderlabs/sqlconnect-go/issues/8)) ([14d8f3f](https://github.com/rudderlabs/sqlconnect-go/commit/14d8f3fb51781fdb158989e52e6b47b42cb9e86d))

## 1.0.0 (2024-03-12)


### Features

* sqlconnect library ([#1](https://github.com/rudderlabs/sqlconnect-go/issues/1)) ([a6fadb5](https://github.com/rudderlabs/sqlconnect-go/commit/a6fadb57e125d397e2e43c78fa5d2df1ea9f2f37))
