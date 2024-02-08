import unittest

from glacius.dsl import add, and_, col, mul, or_, reconstruct, sub, when


class TestDSL(unittest.TestCase):
    def test_compile_recompile_with_and(self):
        """Test that the DSL can compile and recompile expressions with 'and' conditions."""
        feature_expr = (
            when(and_(col("genre") == "comedy", col("stream_type") == "SVOD"))
            .then(1)
            .otherwise(0)
        )
        self.check_compile_recompile(feature_expr)

    def test_compile_recompile_with_or(self):
        """Test that the DSL can compile and recompile expressions with 'or' conditions."""
        feature_expr = (
            when(or_(col("genre") == "comedy", col("genre") == "action"))
            .then(1)
            .otherwise(0)
        )
        self.check_compile_recompile(feature_expr)

    def test_compile_recompile_with_nested_conditions(self):
        """Test that the DSL can compile and recompile expressions with nested conditions."""
        feature_expr = (
            when(
                and_(
                    or_(col("genre") == "comedy", col("genre") == "action"),
                    col("stream_type") == "SVOD",
                )
            )
            .then(1)
            .otherwise(0)
        )
        self.check_compile_recompile(feature_expr)

    def test_compile_recompile_with_addition(self):
        """Test that the DSL can compile and recompile expressions with addition."""
        feature_expr = (
            when(col("genre") == "comedy")
            .then(add(col("streamed_secs"), 10))
            .otherwise(0)
        )
        self.check_compile_recompile(feature_expr)

    def test_compile_recompile_with_subtraction(self):
        """Test that the DSL can compile and recompile expressions with subtraction."""
        feature_expr = (
            when(col("genre") == "comedy")
            .then(sub(col("streamed_secs"), 5))
            .otherwise(0)
        )
        self.check_compile_recompile(feature_expr)

    def test_compile_recompile_with_multiplication(self):
        """Test that the DSL can compile and recompile expressions with multiplication."""
        feature_expr = (
            when(col("genre") == "comedy")
            .then(mul(col("streamed_secs"), 2))
            .otherwise(0)
        )
        self.check_compile_recompile(feature_expr)

    def check_compile_recompile(self, feature_expr):
        """Helper function to compile, reconstruct, and compare feature expressions."""
        compiled_expr = feature_expr.compile()
        try:
            reconstructed_expr = reconstruct(compiled_expr)
            self.assertEqual(compiled_expr, reconstructed_expr.compile())
        except Exception as e:
            self.fail(f"Reconstruction failed: {e}")


if __name__ == "__main__":
    unittest.main()
