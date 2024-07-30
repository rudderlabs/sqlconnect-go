# Changelog

## [1.7.4](https://github.com/rudderlabs/sqlconnect-go/compare/v1.7.3...v1.7.4) (2024-07-30)


### Miscellaneous

* **deps:** bump github.com/docker/docker from 26.1.3+incompatible to 26.1.4+incompatible in the go_modules group ([#151](https://github.com/rudderlabs/sqlconnect-go/issues/151)) ([e9b0b68](https://github.com/rudderlabs/sqlconnect-go/commit/e9b0b68237c03014b17ba14eac97745c9f5c6f96))
* **deps:** bump github.com/rudderlabs/rudder-go-kit from 0.34.2 to 0.35.1 ([#148](https://github.com/rudderlabs/sqlconnect-go/issues/148)) ([8de9c7b](https://github.com/rudderlabs/sqlconnect-go/commit/8de9c7b36d6e4206d682722c40fa84624b0d688a))
* **deps:** bump google.golang.org/api from 0.188.0 to 0.189.0 ([#150](https://github.com/rudderlabs/sqlconnect-go/issues/150)) ([e9631fb](https://github.com/rudderlabs/sqlconnect-go/commit/e9631fb4429e86907bd194f6d216d9298e3ba0bf))

## [1.7.3](https://github.com/rudderlabs/sqlconnect-go/compare/v1.7.2...v1.7.3) (2024-07-29)


### Bug Fixes

* **redshift:** dialect#parserelationref doesn't honour enable_case_sensitive_identifier ([#146](https://github.com/rudderlabs/sqlconnect-go/issues/146)) ([359c230](https://github.com/rudderlabs/sqlconnect-go/commit/359c2304c54acccd7045dabaccf4bfe004cfe5f0))

## [1.7.2](https://github.com/rudderlabs/sqlconnect-go/compare/v1.7.1...v1.7.2) (2024-07-27)


### Miscellaneous

* passing application and login timeout for snowflake config ([#144](https://github.com/rudderlabs/sqlconnect-go/issues/144)) ([c92fd85](https://github.com/rudderlabs/sqlconnect-go/commit/c92fd85c9578054b4e2acaaf2d7a8369e8dbc1fd))

## [1.7.1](https://github.com/rudderlabs/sqlconnect-go/compare/v1.7.0...v1.7.1) (2024-07-24)


### Bug Fixes

* dialect quoting and normalisation inconsistencies ([#143](https://github.com/rudderlabs/sqlconnect-go/issues/143)) ([7a11657](https://github.com/rudderlabs/sqlconnect-go/commit/7a116576a86354585257a710e1497caacce6140a))


### Miscellaneous

* **redshift-data:** assert that rows affected are available after commit ([#141](https://github.com/rudderlabs/sqlconnect-go/issues/141)) ([6b64477](https://github.com/rudderlabs/sqlconnect-go/commit/6b644771f1f536187249c973432a86c155c5959a))

## [1.7.0](https://github.com/rudderlabs/sqlconnect-go/compare/v1.6.0...v1.7.0) (2024-07-22)


### Features

* add dialect#normaliseidentifier and dialect#parserelationref ([#120](https://github.com/rudderlabs/sqlconnect-go/issues/120)) ([31d700d](https://github.com/rudderlabs/sqlconnect-go/commit/31d700d5a6b92a9bf7f27945e96811636ddd0d65))
* **redshift:** add support for assuming an iam role for redshift ([#132](https://github.com/rudderlabs/sqlconnect-go/issues/132)) ([180a706](https://github.com/rudderlabs/sqlconnect-go/commit/180a7068b8b839f5fa763499514aa3a608ce628f))
* **redshift:** bump github.com/aws/aws-sdk-go-v2/service/redshiftdata from 1.27.1 to 1.27.3 ([#137](https://github.com/rudderlabs/sqlconnect-go/issues/137)) ([ec10cc4](https://github.com/rudderlabs/sqlconnect-go/commit/ec10cc476d1f8d698bf8814f5dc3d038563a39ec))


### Miscellaneous

* additional params for databricks driver ([#133](https://github.com/rudderlabs/sqlconnect-go/issues/133)) ([9c34f3f](https://github.com/rudderlabs/sqlconnect-go/commit/9c34f3f935dd84a614e96eb2b07dd5cd090aacff))

## [1.6.0](https://github.com/rudderlabs/sqlconnect-go/compare/v1.5.0...v1.6.0) (2024-07-08)


### Features

* add array data type support for legacy mappings in snowflake ([#123](https://github.com/rudderlabs/sqlconnect-go/issues/123)) ([b564a72](https://github.com/rudderlabs/sqlconnect-go/commit/b564a72a53e6f7fbe86ca149861d88ce62457149))
* **databricks:** bump github.com/databricks/databricks-sql-go from 1.5.6 to 1.5.7 ([#93](https://github.com/rudderlabs/sqlconnect-go/issues/93)) ([96770f4](https://github.com/rudderlabs/sqlconnect-go/commit/96770f4857a6547ab82f09e0cf6d1c66b10cdc7a))
* **redshift:** bump github.com/aws/aws-sdk-go-v2/service/redshiftdata from 1.25.8 to 1.27.1 ([#117](https://github.com/rudderlabs/sqlconnect-go/issues/117)) ([1dfb79d](https://github.com/rudderlabs/sqlconnect-go/commit/1dfb79d09a77625d8ab90351aef4ec33f160300b))


### Miscellaneous

* update code owners ([#124](https://github.com/rudderlabs/sqlconnect-go/issues/124)) ([8478198](https://github.com/rudderlabs/sqlconnect-go/commit/847819862f22b006114ce1f92bd5a5aae550f5b9))

## [1.5.0](https://github.com/rudderlabs/sqlconnect-go/compare/v1.4.1...v1.5.0) (2024-06-20)


### Features

* **redshift:** support external relations ([#108](https://github.com/rudderlabs/sqlconnect-go/issues/108)) ([737f1ed](https://github.com/rudderlabs/sqlconnect-go/commit/737f1ed44c2d974d2a293288126c4d0fd2750739))

## [1.4.1](https://github.com/rudderlabs/sqlconnect-go/compare/v1.4.0...v1.4.1) (2024-06-19)


### Bug Fixes

* **snowflake:** private keys provided through a textarea not being normalised properly ([#106](https://github.com/rudderlabs/sqlconnect-go/issues/106)) ([b15ffc5](https://github.com/rudderlabs/sqlconnect-go/commit/b15ffc524b4b56743a562fead8ec8f8621f1c8c0))

## [1.4.0](https://github.com/rudderlabs/sqlconnect-go/compare/v1.3.1...v1.4.0) (2024-06-18)


### Features

* **snowflake:** support key-pair authentication ([#103](https://github.com/rudderlabs/sqlconnect-go/issues/103)) ([011a7fd](https://github.com/rudderlabs/sqlconnect-go/commit/011a7fd19eae06c0c466756306f6ec58f45d0017))

## [1.3.1](https://github.com/rudderlabs/sqlconnect-go/compare/v1.3.0...v1.3.1) (2024-06-12)


### Bug Fixes

* listcolumns returns an empty list instead of an error in case the relation doesn't exist ([#99](https://github.com/rudderlabs/sqlconnect-go/issues/99)) ([bfe30c7](https://github.com/rudderlabs/sqlconnect-go/commit/bfe30c7b2a899175de89931d7375aa1e191a64a7))


### Miscellaneous

* **redshift:** add tests for non schema binding views ([#92](https://github.com/rudderlabs/sqlconnect-go/issues/92)) ([7c9398c](https://github.com/rudderlabs/sqlconnect-go/commit/7c9398c985afeeb69791061a3e40ccb0fe4783bb))

## [1.3.0](https://github.com/rudderlabs/sqlconnect-go/compare/v1.2.0...v1.3.0) (2024-06-07)


### Features

* **databricks:** bump github.com/databricks/databricks-sql-go from 1.5.5 to 1.5.6 ([#86](https://github.com/rudderlabs/sqlconnect-go/issues/86)) ([b20a89e](https://github.com/rudderlabs/sqlconnect-go/commit/b20a89e1eaa8a3377678a223270c1356cc47c4c2))
* **snowflake:** bump github.com/snowflakedb/gosnowflake from 1.10.0 to 1.10.1 ([#88](https://github.com/rudderlabs/sqlconnect-go/issues/88)) ([f210281](https://github.com/rudderlabs/sqlconnect-go/commit/f210281f04b0df5f8c6c66845811330c1133ed07))


### Bug Fixes

* **redshift:** listcolumns doesn't return any results for views with no schema binding ([#91](https://github.com/rudderlabs/sqlconnect-go/issues/91)) ([9489790](https://github.com/rudderlabs/sqlconnect-go/commit/9489790010455d84c60b7d6a04c649ad2cddb83a))

## [1.2.0](https://github.com/rudderlabs/sqlconnect-go/compare/v1.1.7...v1.2.0) (2024-05-20)


### Features

* **bigquery:** bump cloud.google.com/go/bigquery from 1.59.1 to 1.61.0 ([#56](https://github.com/rudderlabs/sqlconnect-go/issues/56)) ([30e3030](https://github.com/rudderlabs/sqlconnect-go/commit/30e303055c64a65179a295caa0d8d510e16a9466))
* **databricks:** bump github.com/databricks/databricks-sql-go from 1.5.3 to 1.5.4 ([#61](https://github.com/rudderlabs/sqlconnect-go/issues/61)) ([ec3fb29](https://github.com/rudderlabs/sqlconnect-go/commit/ec3fb29625866dc9c1f82e077b659af32795d824))
* **databricks:** bump github.com/databricks/databricks-sql-go from 1.5.4 to 1.5.5 ([#66](https://github.com/rudderlabs/sqlconnect-go/issues/66)) ([000e3b4](https://github.com/rudderlabs/sqlconnect-go/commit/000e3b4b926ff62c7c3527261fdf395c734fc560))
* **mysql:** bump github.com/go-sql-driver/mysql from 1.7.1 to 1.8.1 ([#11](https://github.com/rudderlabs/sqlconnect-go/issues/11)) ([de51b3a](https://github.com/rudderlabs/sqlconnect-go/commit/de51b3a3445fa06e942eb70cff5f74e9adbaa931))
* **redshift:** bump github.com/aws/aws-sdk-go-v2/service/redshiftdata from 1.25.4 to 1.25.7 ([#75](https://github.com/rudderlabs/sqlconnect-go/issues/75)) ([3036555](https://github.com/rudderlabs/sqlconnect-go/commit/3036555591b3273403a47ceb0d467f5275f86656))
* **snowflake:** bump github.com/snowflakedb/gosnowflake from 1.7.2 to 1.9.0 ([#33](https://github.com/rudderlabs/sqlconnect-go/issues/33)) ([5b926c8](https://github.com/rudderlabs/sqlconnect-go/commit/5b926c83908c7d99992ee2d892edbd1a526ec8e3))
* **snowflake:** bump snowflake to v1.10.0 ([#79](https://github.com/rudderlabs/sqlconnect-go/issues/79)) ([b205e59](https://github.com/rudderlabs/sqlconnect-go/commit/b205e59b30ab3b2505983b00559b58e5c71daa4f))
* **trino:** bump github.com/trinodb/trino-go-client from 0.313.0 to 0.315.0 ([#64](https://github.com/rudderlabs/sqlconnect-go/issues/64)) ([b15133b](https://github.com/rudderlabs/sqlconnect-go/commit/b15133b0429905024d5603abf6a41ead35f906ee))


### Miscellaneous

* **bigquery:** downgrade bigquery from 1.61.0 to 1.60.0 ([#77](https://github.com/rudderlabs/sqlconnect-go/issues/77)) ([eda4cf6](https://github.com/rudderlabs/sqlconnect-go/commit/eda4cf6de926667256969ed8d51d48abceb2ffea))
* bump bigquery to v1.61.0 ([#78](https://github.com/rudderlabs/sqlconnect-go/issues/78)) ([3ad5159](https://github.com/rudderlabs/sqlconnect-go/commit/3ad5159212b26e09753388504041157b952c1ed3))

## [1.1.7](https://github.com/rudderlabs/sqlconnect-go/compare/v1.1.6...v1.1.7) (2024-04-29)


### Miscellaneous

* **deps:** bump github.com/aws/aws-sdk-go-v2/config from 1.25.3 to 1.27.11 ([#60](https://github.com/rudderlabs/sqlconnect-go/issues/60)) ([e30bf0c](https://github.com/rudderlabs/sqlconnect-go/commit/e30bf0c7b398e3c2a089595e3cdc5e6aa4a335e1))
* **deps:** bump golangci/golangci-lint-action from 4 to 5 ([#59](https://github.com/rudderlabs/sqlconnect-go/issues/59)) ([22be313](https://github.com/rudderlabs/sqlconnect-go/commit/22be313b48f3e7fe2a36e68ef92ab4a5df4eef88))

## [1.1.6](https://github.com/rudderlabs/sqlconnect-go/compare/v1.1.5...v1.1.6) (2024-04-26)


### Miscellaneous

* use go 1.22 with toolchain ([#57](https://github.com/rudderlabs/sqlconnect-go/issues/57)) ([abbeb68](https://github.com/rudderlabs/sqlconnect-go/commit/abbeb68ebc242dccf35e1016e28d0718fb32bff5))

## [1.1.5](https://github.com/rudderlabs/sqlconnect-go/compare/v1.1.4...v1.1.5) (2024-04-26)


### Miscellaneous

* **databricks:** add configuration option for setting the default schema ([#54](https://github.com/rudderlabs/sqlconnect-go/issues/54)) ([17cdafc](https://github.com/rudderlabs/sqlconnect-go/commit/17cdafc80f53ecbe96be38ad171fbe0cb04464c5))
* **deps:** bump cloud.google.com/go from 0.112.1 to 0.112.2 ([#51](https://github.com/rudderlabs/sqlconnect-go/issues/51)) ([f19c0be](https://github.com/rudderlabs/sqlconnect-go/commit/f19c0be123b7f76c360a3f76ac5fb982cea820a9))
* **deps:** bump google.golang.org/api from 0.172.0 to 0.175.0 ([#52](https://github.com/rudderlabs/sqlconnect-go/issues/52)) ([8eeea74](https://github.com/rudderlabs/sqlconnect-go/commit/8eeea74b3b7a3f78d5a490de80321d83bd7b45d4))
* **deps:** bump rudder-go-kit to v0.29.0 ([#55](https://github.com/rudderlabs/sqlconnect-go/issues/55)) ([62df998](https://github.com/rudderlabs/sqlconnect-go/commit/62df998df6186ffa8f5bcfb84025f4b8fe3d8107))

## [1.1.4](https://github.com/rudderlabs/sqlconnect-go/compare/v1.1.3...v1.1.4) (2024-04-16)


### Bug Fixes

* redshift-data driver inconsistencies ([#45](https://github.com/rudderlabs/sqlconnect-go/issues/45)) ([9df88a6](https://github.com/rudderlabs/sqlconnect-go/commit/9df88a6707dff19d9a72b7e37053ac44ea246e70))


### Miscellaneous

* add CODEOWNERS file ([#46](https://github.com/rudderlabs/sqlconnect-go/issues/46)) ([55f395f](https://github.com/rudderlabs/sqlconnect-go/commit/55f395fe7f873af47e3635b6d06d0ad200790f42))
* **deps:** bump github.com/rudderlabs/rudder-go-kit from 0.24.0 to 0.27.0 ([#48](https://github.com/rudderlabs/sqlconnect-go/issues/48)) ([b0bd18e](https://github.com/rudderlabs/sqlconnect-go/commit/b0bd18e8741151b8b5ed131dc9dca5bc98e3bd70))
* **redshift-data:** add support for RetryMaxAttempts with default value set to 20 ([#50](https://github.com/rudderlabs/sqlconnect-go/issues/50)) ([15e0220](https://github.com/rudderlabs/sqlconnect-go/commit/15e0220d34a4850edeb83639de76bfc1bb5df2a9))

## [1.1.3](https://github.com/rudderlabs/sqlconnect-go/compare/v1.1.2...v1.1.3) (2024-04-09)


### Miscellaneous

* **bigquery:** add support for retrying jobRateLimitExceeded errors ([#39](https://github.com/rudderlabs/sqlconnect-go/issues/39)) ([314f5eb](https://github.com/rudderlabs/sqlconnect-go/commit/314f5eb31cd6128e012901eac193ff05f992a186))

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
