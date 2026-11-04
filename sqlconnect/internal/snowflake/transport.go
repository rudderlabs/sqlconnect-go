package snowflake

import "net/http"

// opaqueTransport prevents gosnowflake/v2 from type-asserting our pool-shared
// *http.Transport and writing TLSClientConfig on every connection open.
type opaqueTransport struct{ *http.Transport }
