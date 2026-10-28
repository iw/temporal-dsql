.PHONY: clean-dsql-bins

bins: temporal-dsql-tool

clean-bins: clean-dsql-bins

clean-dsql-bins:
	@rm -f temporal-dsql-tool

temporal-dsql-tool: $(ALL_SRC)
	@printf $(COLOR) "Build temporal-dsql-tool with CGO_ENABLED=$(CGO_ENABLED) for $(GOOS)/$(GOARCH)..."
	CGO_ENABLED=$(CGO_ENABLED) go build $(BUILD_TAG_FLAG) -o temporal-dsql-tool ./cmd/tools/dsql
