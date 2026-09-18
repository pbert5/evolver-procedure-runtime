# evolver-procedure-runtime

Standalone package for the generic, session-local interactive procedure
runtime used by Meta BAL and `evoctl`.

The public import namespace is `evolver_procedure_runtime`. This component
owns procedure models, compilation, execution, events, invoker interfaces,
and sink contracts. eVOLVER-specific scientific workflow manifests remain
owned by Meta BAL.

The package was extracted from Meta BAL's root `procedure/` component with
history preserved through the initial subtree import. Runtime semantics are
unchanged by the packaging move.
