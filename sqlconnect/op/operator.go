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

	// array includes at least one of the values
	ContainsAny Operator = "contains_any"
	// array includes all of the values
	ContainsAll Operator = "contains_all"
	// array does not include the value
	NContains Operator = "ncontains"
	// array has zero elements
	Empty Operator = "empty"
	// array has one or more elements
	Nempty Operator = "nempty"
	// array element count = n
	SizeEq Operator = "size_eq"
	// array element count > n
	SizeGt Operator = "size_gt"
	// array element count < n
	SizeLt Operator = "size_lt"
)

func IsValid(op Operator) bool {
	switch op {
	case Eq, Neq, In, Nin, Gt, Gte, Lt, Lte, Like, NLike, Btw, Nbtw, Inlast, Nnull, Null,
		ContainsAny, ContainsAll, NContains, Empty, Nempty, SizeEq, SizeGt, SizeLt:
		return true
	}
	return false
}
