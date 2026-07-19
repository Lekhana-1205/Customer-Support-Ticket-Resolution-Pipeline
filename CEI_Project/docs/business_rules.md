# Business Rules

## Rule 1

Only tickets with status **Resolved** are considered.

---

## Rule 2

Resolution time must be greater than **15 minutes**.

---

## Rule 3

Resolution time stored as

```
Xh Xm Xs
```

is converted into total minutes.

If seconds are greater than or equal to 30, the value is rounded up.

---

## Rule 4

Only Team Leads

- TL01
- TL02
- TL03
- TL04
- TL05
- TL06
- TL07
- TL08

are included.

---

## Rule 5

Agents who already qualified on Day 1 are excluded from Day 2 analysis to avoid double counting.