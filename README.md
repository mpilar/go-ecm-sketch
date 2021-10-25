# go-ecm-sketch

Simple implementation of [ECM-Sketch](https://arxiv.org/pdf/1207.0139.pdf), a time/count-windowed probabilistic counting data structure. 

ADD USAGE HERE (but main.go is a starting point).

What this will allow you to do is with some error, count any number of items. The count itself will be inaccurate, but the relative counts should be fairly accurate. This will allow ranking of the different items.

Eventually a variation will be created to internally keep track of the top-k items across the windows.

TODO:
- add a top-k structure to keep track of the top-k elements in the last N counts