# Go file and workflow example convention

Keep workflow example files in this order:

1. `package` declaration and imports.
2. A documentation block with `Name` and `Funktion`.
3. The workflow name as a constant.
4. The documented `StartToken` type.
5. The documented `Result` type.
6. The remaining implementation.

Put the workflow description in the name constant's Go doc comment, beginning
with the constant identifier and including `Name` and `Funktion` lines. Give
the start token and result explicit, documented types. Build the start token
with the token builder and write the final output with `SetResult`; decode
those values through the corresponding reader methods. The README workflow
example is the reference layout.
