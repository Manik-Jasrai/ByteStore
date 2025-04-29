package utils

func Assert(val bool, msg string) {
	if !val {
		panic(msg)
	}
}
