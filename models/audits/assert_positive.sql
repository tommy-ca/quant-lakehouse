AUDIT (
  name assert_positive,
);
SELECT * FROM @this WHERE volume < 0 OR volume IS NULL
