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
	NotIn Operator = "notin"

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
	NotLike Operator = "notlike"
	// left <= v <= right
	Between Operator = "between"
	// v < left OR v > right
	NotBetween Operator = "notbetween"

	// IS NOT NULL
	IsSet Operator = "isset"
	// IS NULL
	NotSet Operator = "notset"
)

func IsValid(op Operator) bool {
	switch op {
	case Eq, Neq, In, NotIn, Gt, Gte, Lt, Lte, Like, NotLike, Between, NotBetween, IsSet, NotSet:
		return true
	}
	return false
}
