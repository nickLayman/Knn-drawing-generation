# Provenance

The enumeration code in this repository was extracted from the
`graph-drawings` research repository at commit
`7dddf13dddb499d88c2e45f2f7b9d2bab2bf6dc0`.

The complete `K(4,4)` drawing and flag census was run from commit
`15e3a7b09b91ebcc43617332503570efdf3fb80f`.  The extracted enumeration
dependency closure is unchanged between that commit and the extraction
commit.

Nova job `16604982` generated the verified census:

- 9,019,729 strong drawing classes allowing a bipartition-side swap;
- 18,031,850 color-preserving strong drawing classes;
- 1,931,529 color-preserving crossing-pair flag classes;
- 967,452 crossing-pair flag classes allowing a side swap;
- 123,926 color-preserving 4-graph flag classes; and
- 62,415 4-graph flag classes allowing a side swap.

The exact run artifacts are stored outside this source repository under the
research data root.  Generated drawings, shards, reducer outputs, and scratch
files are intentionally not stored in Git.
