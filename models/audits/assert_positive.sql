AUDIT (
  name assert_positive,
);
SELECT * FROM @this WHERE @column < 0 OR @column IS NULL
