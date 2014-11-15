dblp
====

Parse the dblp data into a structured format for experimentation.

# Format (V7)

    #* --- paperTitle
    #@ --- Authors
    #t ---- Year
    #c  --- publication venue
    #index 00---- index id of this paper
    #% ---- the id of references of this paper (there are multiple lines, with each indicating a reference)
    #! --- Abstract

The following is an example:

    #*Information geometry of U-Boost and Bregman divergence
    #@Noboru Murata,Takashi Takenouchi,Takafumi Kanamori,Shinto Eguchi
    #t2004
    #cNeural Computation
    #index436405
    #%94584
    #%282290
    #%605546
    #%620759
    #%564877
    #%564235
    #%594837
    #%479177
    #%586607
    #!We aim at an extension of AdaBoost to U-Boost, in the paradigm to build a stronger classification machine from a set of weak learning machines. A geometric understanding of the Bregman divergence defined by a generic convex function U leads to the U-Boost method in the framework of information geometry extended to the space of the finite measures over a label set. We propose two versions of U-Boost learning algorithms by taking account of whether the domain is restricted to the space of probability functions. In the sequential step, we observe that the two adjacent and the initial classifiers are associated with a right triangle in the scale via the Bregman divergence, called the Pythagorean relation. This leads to a mild convergence property of the U-Boost algorithm as seen in the expectation-maximization algorithm. Statistical discussions for consistency and robustness elucidate the properties of the U-Boost methods based on a stochastic assumption for training data.

# Name Disambiguation

There are clearly quite a few author names which are written in different ways
for different papers. The most obvious examples are those where the first or
second name is abbreviated with a single letter in one place and spelled out
fully in another. Another that is quite common is when hyphenated names are
spelled in some places with the hyphen and in some without. There are also
simple common misspellings, although these are harder to detect, since an edit
distance of 1 or 2 could just as easily be a completely different name. One case
which might be differentiated is when the edit is a deletion of a letter in a
string of one or more of that same letter. For instance, "Acharya" vs.
"Acharyya". Here it likely the second spelling simply has an extraneous y.

# From PostgresSQL to Neo4j

http://neo4j.com/developer/guide-importing-data-and-etl/
