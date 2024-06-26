package set

type Set[T comparable] struct {
	m map[T]struct{}
}

func NewSet[T comparable]() *Set[T] {
	return &Set[T]{m: make(map[T]struct{})}
}

func (s *Set[T]) Add(value T) {
	s.m[value] = struct{}{}
}

func (s *Set[T]) Slice() []T {
	res := make([]T, 0, len(s.m))
	for val := range s.m {
		res = append(res, val)
	}
	return res
}
