package clusterconfig_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/internal/clusterconfig"
)

var _ = Describe("ParseResourceAttributes", func() {
	It("percent-decodes keys and values", func() {
		got, err := clusterconfig.ParseResourceAttributes("a%20b=c%2Cd,k%3Dx=v%3Dw")
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(map[string]string{"a b": "c,d", "k=x": "v=w"}))
	})

	DescribeTable("rejects the whole value on",
		func(in string) {
			got, err := clusterconfig.ParseResourceAttributes(in)
			Expect(err).To(HaveOccurred())
			Expect(got).To(BeNil())
			Expect(err.Error()).NotTo(ContainSubstring("secretvalue"))
		},
		Entry("a bad escape", "a=1,b=%zzsecretvalue"),
		Entry("a missing '='", "a=1,bsecretvalue"),
		Entry("an empty key", "a=1,=secretvalue"),
		Entry("a bad escape in a key", "%zz=secretvalue"),
		Entry("a trailing comma", "a=1,"),
	)

	It("trims spaces around keys and values", func() {
		got, err := clusterconfig.ParseResourceAttributes(" a = b , c=d ")
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(map[string]string{"a": "b", "c": "d"}))
	})

	It("accepts an empty value and an empty attribute value", func() {
		got, err := clusterconfig.ParseResourceAttributes("")
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(BeEmpty())
		got, err = clusterconfig.ParseResourceAttributes("a=")
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(map[string]string{"a": ""}))
	})
})
