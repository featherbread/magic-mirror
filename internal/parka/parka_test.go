package parka_test

func makeIntKeys(n int) []int {
	keys := make([]int, n)
	for i := range keys {
		keys[i] = i
	}
	return keys
}

func maxOfChannel(ch <-chan int) int {
	var maxI int
	for i := range ch {
		maxI = max(i, maxI)
	}
	return maxI
}

func promise(fn func()) <-chan struct{} {
	done := make(chan struct{})
	go func() { defer close(done); fn() }()
	return done
}
