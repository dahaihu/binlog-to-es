package utils


type Set map[string]struct{}

func NewSet(values []string) Set {
	set := make(Set, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	return set
}

func (s Set) Contains(value string) bool {
	_, ok := s[value]
	return ok
}

func (s Set) Empty() bool {
	return len(s) == 0
}
