package op

type Operator string

const (
	// =
	Eq Operator = "eq"
	// !=
	Neq Operator = "neq"
	// IN (...)
	In Operator = "in"
	// NOT IN (...)
	Nin Operator = "nin"

	// >
	Gt Operator = "gt"
	// >=
	Gte Operator = "gte"
	// <
	Lt Operator = "lt"
	// <=
	Lte Operator = "lte"

	// LIKE
	Like Operator = "like"
	// NOT LIKE
	NLike Operator = "nlike"
	// left <= v <= right
	Btw Operator = "btw"
	// v < left OR v > right
	Nbtw Operator = "nbtw"
	// left >= now() - INTERVAL right
	Inlast Operator = "inlast"

	// IS NULL
	Null Operator = "null"

	// IS NOT NULL
	Nnull Operator = "nnull"
)

func IsValid(op Operator) bool {
	switch op {
	case Eq, Neq, In, Nin, Gt, Gte, Lt, Lte, Like, NLike, Btw, Nbtw, Inlast, Nnull, Null:
		return true
	}
	return false
}
