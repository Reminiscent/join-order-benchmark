# Generated Join graph

This file is generated for quick inspection. `graph.json` remains the
authoritative machine-readable graph.

```mermaid
graph LR
    R0["R0<br/>rows=631,707"]
    R1["R1<br/>rows=13,873"]
    R2["R2<br/>rows=1,491"]
    R3["R3<br/>rows=748,299"]
    R4["R4<br/>rows=34,829"]
    R0 ---|"one_to_many<br/>output ×0.0262308"| R2
    R1 ---|"many_to_many<br/>output ×0.549927"| R3
    R1 ---|"one_to_many<br/>output ×0.0369577"| R4
    R2 ---|"one_to_many<br/>output ×0.248574"| R4
```

## Edge details

For each endpoint, `D` is its key NDV and `M = rows / D` is its
average key multiplicity. `q` is the effective fraction of the
smaller key set that matches the other endpoint. The logical
selectivity is `q / max(D_left, D_right)`.

| Edge | Type | Left `D` | Left `M` | Right `D` | Right `M` | Overlap `q` | Selectivity | Output factor |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| R0 -- R2 | `one_to_many` | 928 | 680.719 | 1,491 | 1 | 0.0262308 | 1.75928e-05 | 0.0262308 |
| R1 -- R3 | `many_to_many` | 3,580 | 3.87514 | 2,751 | 272.01 | 0.141911 | 3.96401e-05 | 0.549927 |
| R1 -- R4 | `one_to_many` | 13,873 | 1 | 11,857 | 2.93742 | 0.0369577 | 2.664e-06 | 0.0369577 |
| R2 -- R4 | `one_to_many` | 1,491 | 1 | 225 | 154.796 | 0.248574 | 0.000166716 | 0.248574 |
