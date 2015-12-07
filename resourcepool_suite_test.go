package resourcepool

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestResourcepool(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Resourcepool Suite")
}
