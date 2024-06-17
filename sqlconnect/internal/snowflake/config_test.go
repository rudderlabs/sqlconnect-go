package snowflake

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParsePrivateKey(t *testing.T) {
	testCases := []struct {
		name       string
		privateKey string
		passPhrase string
		wantError  bool
	}{
		{
			name:       "valid private key with valid passphrase",
			privateKey: `-----BEGIN ENCRYPTED PRIVATE KEY----- MIIFJDBWBgkqhkiG9w0BBQ0wSTAxBgkqhkiG9w0BBQwwJAQQh/r9Tt8BEe/IRV59 9/+WZQICCAAwDAYIKoZIhvcNAgkFADAUBggqhkiG9w0DBwQIv4X4Tl3JDUoEggTI UwkI7WrLrKGlTA46KBKc9UXejLcMSghlhQGv0T9CW7tLsrH3vR7VO1Hkh6iHdPef Ir1wU3iH9etNDgHvr6sEe4p8v9FCHWicxkVbVWtMugT4iT+ejGjnxaXyUsWF4Ker o+2c7jVpYS1mIJhxPdXd9acFGoLe2Lhhe+yfskPbmiCc8mbHDzxFx7vMsS3klF44 RCfdXC2rcuHkesjmd6sMXhB0B6xKGgDxYUodiK5axJr6hFZusPEllZTeMZtVbWXd w/nFv4L7un3bBnzIkAL5EQHe+jGMmNTaT/wf+zsoQkXlYX/UXNIqZ1M0X7w8ZskH mwkX43vQDzSqQ5lkBpFCPb2cYK6OfxEs+ToaQBdMhBxyhJqi/1keokbuQZGGQPBV coxkFlNczVkGAKpFC4MFI20vf1bBNrqTzUG9AFmZRfzCo6AWkmR7zQZ6eAigxgTk IdNne2BXY2bi919ytRNzSWd7Wwhiwm7niTKtP2BJjEfTsfIZ0KiXGN4C6J8wODk3 CAaRcHELVWxXFgKSnWkXgJZUq02QG00LZnQuBEZnjioj8fEEuHjey3FRqQaXrSoe ewyn/qZNepxFvkeLJu1fcVGwSsNxQzxJ3FRT6uVGP22+wN6ZZBL0SBiM+z7ndakx rpa/Or4+amPcBFYyDbed5vN9eB6V1xN9t0zarARAqiMy+h8uFm3xKTrNXcatjub2 SAEFl7vaQY1nq+i8eX+JYzYGnCpGw+p+cXwfeOxYLg4aCravMzxR1aGpynYSPOy6 X5kFX5eKNYNM/FRenzJlHDbFmV9cBxC9L2j2aUJhwUeFSJD+SVW5KCdwjj9VYVTg 4uJFODv+KurNwcx4w2HcmVnC0Yahb0JzvNJ4VQ1Yg2//jeYaS2cxDHigUFTIwtBy IRU/T48dbnpNuaA1/OgA3/b9Kxy+RRCH6sgiFhY+clRz4hTn3uEhIJhV2iycTPlS 4kfOUVMRsdFYiMVpA9sfq7z/nwDQjBBqgktQrVsCOVNnI/tgZhguJYTltkNbqI8v YHWw/ag+TBGbk5WjqHQMmXhvq7Wp9Bl6b0oP1OGtdrQEaHdTPdQ1gTpAXEhPpMpl GNhGwK4DSol8VsBkRDICqv56ECoHrtBuvo3Kl6pBVCBvOuh9ZExKhHHOcd0zj0AH 1vGnn0xp7Jj7p0kslt/YVc7fN9xU9h8Om98LnR8/OXC0uRIO1cuotOaTCMfjz2Ts 7N3cM3Le0gVC/gbcCqVUqetgMF0jfuQoeoZyuG/e6dM39n6jnTcuug7NBASXMKey QzZW04IjI0EuBzQvYcPu47mRVzcd1QFWw8Fr/zo5ZKo8M4UGwgbJwDTqQTOpQEcv bMGbTxjs/RSWe3YUe239OITM6F0b7WlEjfkDFnB+Xys2DE9GC2wZlQQ6mo0Ver2x ta5MSkiWWvdTmRYI7L/K7KJQjOGInrLuugx+/N8KQbuiUZB9+D/FyNBVdL4S73BA IzMhbHcN1CKH8uB+18L7t91VLuJigi3f0lAWM+QNW36RUZzn2LtlbJ5nnlZRa73t VLk1y43Penk1djaF6bk3Em0GXBlPiCcTwlOZfIb543IWCkxBeX/WmmaoeNB10qoL +qr8ukOxkKhDksWc7fsfno1RzeifSTsA -----END ENCRYPTED PRIVATE KEY-----`,
			passPhrase: "oW$47MjPgr$$Lc",
		},
		{
			name:       "valid private key with invalid passphrase",
			privateKey: `-----BEGIN ENCRYPTED PRIVATE KEY----- MIIFJDBWBgkqhkiG9w0BBQ0wSTAxBgkqhkiG9w0BBQwwJAQQh/r9Tt8BEe/IRV59 9/+WZQICCAAwDAYIKoZIhvcNAgkFADAUBggqhkiG9w0DBwQIv4X4Tl3JDUoEggTI UwkI7WrLrKGlTA46KBKc9UXejLcMSghlhQGv0T9CW7tLsrH3vR7VO1Hkh6iHdPef Ir1wU3iH9etNDgHvr6sEe4p8v9FCHWicxkVbVWtMugT4iT+ejGjnxaXyUsWF4Ker o+2c7jVpYS1mIJhxPdXd9acFGoLe2Lhhe+yfskPbmiCc8mbHDzxFx7vMsS3klF44 RCfdXC2rcuHkesjmd6sMXhB0B6xKGgDxYUodiK5axJr6hFZusPEllZTeMZtVbWXd w/nFv4L7un3bBnzIkAL5EQHe+jGMmNTaT/wf+zsoQkXlYX/UXNIqZ1M0X7w8ZskH mwkX43vQDzSqQ5lkBpFCPb2cYK6OfxEs+ToaQBdMhBxyhJqi/1keokbuQZGGQPBV coxkFlNczVkGAKpFC4MFI20vf1bBNrqTzUG9AFmZRfzCo6AWkmR7zQZ6eAigxgTk IdNne2BXY2bi919ytRNzSWd7Wwhiwm7niTKtP2BJjEfTsfIZ0KiXGN4C6J8wODk3 CAaRcHELVWxXFgKSnWkXgJZUq02QG00LZnQuBEZnjioj8fEEuHjey3FRqQaXrSoe ewyn/qZNepxFvkeLJu1fcVGwSsNxQzxJ3FRT6uVGP22+wN6ZZBL0SBiM+z7ndakx rpa/Or4+amPcBFYyDbed5vN9eB6V1xN9t0zarARAqiMy+h8uFm3xKTrNXcatjub2 SAEFl7vaQY1nq+i8eX+JYzYGnCpGw+p+cXwfeOxYLg4aCravMzxR1aGpynYSPOy6 X5kFX5eKNYNM/FRenzJlHDbFmV9cBxC9L2j2aUJhwUeFSJD+SVW5KCdwjj9VYVTg 4uJFODv+KurNwcx4w2HcmVnC0Yahb0JzvNJ4VQ1Yg2//jeYaS2cxDHigUFTIwtBy IRU/T48dbnpNuaA1/OgA3/b9Kxy+RRCH6sgiFhY+clRz4hTn3uEhIJhV2iycTPlS 4kfOUVMRsdFYiMVpA9sfq7z/nwDQjBBqgktQrVsCOVNnI/tgZhguJYTltkNbqI8v YHWw/ag+TBGbk5WjqHQMmXhvq7Wp9Bl6b0oP1OGtdrQEaHdTPdQ1gTpAXEhPpMpl GNhGwK4DSol8VsBkRDICqv56ECoHrtBuvo3Kl6pBVCBvOuh9ZExKhHHOcd0zj0AH 1vGnn0xp7Jj7p0kslt/YVc7fN9xU9h8Om98LnR8/OXC0uRIO1cuotOaTCMfjz2Ts 7N3cM3Le0gVC/gbcCqVUqetgMF0jfuQoeoZyuG/e6dM39n6jnTcuug7NBASXMKey QzZW04IjI0EuBzQvYcPu47mRVzcd1QFWw8Fr/zo5ZKo8M4UGwgbJwDTqQTOpQEcv bMGbTxjs/RSWe3YUe239OITM6F0b7WlEjfkDFnB+Xys2DE9GC2wZlQQ6mo0Ver2x ta5MSkiWWvdTmRYI7L/K7KJQjOGInrLuugx+/N8KQbuiUZB9+D/FyNBVdL4S73BA IzMhbHcN1CKH8uB+18L7t91VLuJigi3f0lAWM+QNW36RUZzn2LtlbJ5nnlZRa73t VLk1y43Penk1djaF6bk3Em0GXBlPiCcTwlOZfIb543IWCkxBeX/WmmaoeNB10qoL +qr8ukOxkKhDksWc7fsfno1RzeifSTsA -----END ENCRYPTED PRIVATE KEY-----`,
			passPhrase: "abc",
			wantError:  true,
		},
		{
			name:       "valid private key without passphrase",
			privateKey: `-----BEGIN PRIVATE KEY----- MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCf6c2HKc84K+Vr hmla9vy1VJICWXGBd7y8EIK2pEc7kCci8z1ZnaXjSpGXgWS3y8IF/DNW+Cxys/yj fyEU5EI47ARqFjzURXRPST74MdZJHKwVP7NlzNBTI/2sb7AqYnVjEWalV24upykq BAyyXrUj06a3lRSQwLhax2jK2InsvPSe9ENOTTEB5vJW7k5k5aSPPH1KPrIlEZRK ymhgWhBa2MvREWe8Jq/BXw9GuYwhcbLrfknI30kNGW1/qvd03JKvQa8nHpxD2fdn HiAbz8pbuA8IKQMVQ0n4VJeFT3+pMIKpGu6Vm9owLteMozVyK+YvI4PzkRWIk6zw HTAbZo5vAgMBAAECggEADcy300Os4ayMEkDZo6NvwFgpd3FvhZwnGdWU6hz4FrBE aFQ0RaEAmUIsmTXt0pyPREP0zDsDXuygTx2f5bUi79WSNfNwUWMi+9qWyAVI+Cs0 wGqsWQsZKSuQbwp+WdIATknIoVkPpZAAUeNikxvwJsTTfMEtMqam4hKWPPb9xAOR XZSZNcslO51eUznlu7baAWx+mIDIK+VacpneL6Fv5u8gS1yNZscYX1pb2cSzyevR ZD/z3wJStxK2HlWhtMY/Wr9f6jSSNY0ldWhsssGzVrAGKMlP6KSCL+XzHqp7r5yA 3L6glIDGnjVwB+OHMPW4JdCd8eXGK8HYxFLEk1JydQKBgQDXTO5+6uB6HayPyJEr pMJ/cRksWGvzxdnsK4xEmgZQu2vNP3BMUGc4PNldRPmM/FH1pkp8KcjK2OFVLHIP zovqQrBVCEVQ+t+5IP6QX/2n76Bb5sSK0O+Fq0fS0LgURHjnr54atI0ziMeT6z32 rThyiE/kpJCg/1zpc7vVJ17QWwKBgQC+JIJwMvlr63dK7FNFCMMgZcsjRYwwbvI0 IX3iKYVy4XHIQCh2UnHOixNG8qD8sfDOrAH7nPObCvxEjC2Eyy+hed2SczO3VCRc zZvVY6ungiSnE2JPkzqhIj633gzYaVkusBb84kkyWC+ZZOUvW19zZrIi9pC8h5Vj 8ek5iwkWfQKBgGrdC4/BYzQZoHopkiy4dbWt3FHPfZ2cuaLoppGyZaoSrNpOP54R VnpqcXVC9B6Patrj9BqW3swYRBfznJXN7lKTUVSTa1xbeUo5X0En9A4z+UNEUo+Y TxrovhiccpHUvrI4z9/veBp5LJ515+aVaewnTohtSkAvH93cDQIqrXv7AoGBALJN akPsiRg6ZlNL6YoC/XeT/TnGLf/9CgL4pSM/7HQeFKTEBS1vgmk84YbWX0CXXElx 4yoftBDf7FAbY1PzdWbm8HA0t3pi3PZpmIgyPvWFhPlno/kbBw+zHT0ubL1DjO3L EsNxL1KWf4xIoOIXvRpqYwGGVZN1URG3+AyN5KfBAoGBALoPqHzSTggaQz+SCgex qNJpuc/224cullUBkwB/iCUYDM3kXYGppoCilpwz8tTnJji/ZSVv1OX/pL+vO+NZ nD6JTI2veDQKvBkG9IaIG4uiwfpXsrNmo4yB4d7PowWcH/orhjFxbEAVIBNKWBtO 55TGyTE3i7XAQXet5g1KP7Zp -----END PRIVATE KEY-----`,
		},
		{
			name:       "invalid private key",
			privateKey: `abc`,
			wantError:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := Config{
				PrivateKey:           tc.privateKey,
				PrivateKeyPassphrase: tc.passPhrase,
			}
			_, err := c.ParsePrivateKey()
			if tc.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
