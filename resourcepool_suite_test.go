package resourcepool

import (
	"runtime"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestResourcepool(t *testing.T) {
	runtime.GOMAXPROCS(1)

	RegisterFailHandler(Fail)
	RunSpecs(t, "Resourcepool Suite")
}
