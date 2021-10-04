package plan

type Rule interface {
	Apply(plan Plan) Plan
}
