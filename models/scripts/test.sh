#!/usr/bin/env bash
#restful api test scripts.

#get model status
curl localhost:8501/angelServing/v1.0/models/lr/versions/6

#angel model prediction test.
curl -H "Content-Type: application/json" -X POST -d '{"instances": [[0.51483303, 0.99900955, 0.9477888, 0.6912188, 0.41446745, 0.2525878, 0.6014038, 0.46847868, 0.12854028, 0.8306037, 0.3461753, 0.1129151, 0.6229094, 0.90299904, 0.50834644, 0.34843314, 0.95900637, 0.9437762, 0.31707388, 0.73501045, 0.05600065, 0.47225082, 0.28908283, 0.7371853, 0.55928135, 0.81367457, 0.91782594, 0.008230567, 0.9317811, 0.0061050057, 0.7060979, 0.51740277, 0.07297987, 0.34826308, 0.43395072, 0.5017575, 0.73248106, 0.7576818, 0.43087876, 0.9380423, 0.5226082, 0.9813176, 0.20717019, 0.42229313, 0.8274106, 0.6791944, 0.48174334, 0.77374876, 0.56179315, 0.6584269, 0.7635249, 0.9949779, 0.84034514, 0.7586089, 0.74443096, 0.21172583, 0.7850719, 0.5341459, 0.84134424, 0.06459451, 0.1270392, 0.41439575, 0.98234355, 0.5515572, 0.9594097, 0.18379861, 0.8221523, 0.23739898, 0.07713032, 0.66251403, 0.84977543, 0.905998, 0.21836805, 0.40002906, 0.6271626, 0.37708586, 0.20958215, 0.051997364, 0.6841619, 0.22454417, 0.34285623, 0.19205093, 0.35783356, 0.29280972, 0.19194472, 0.42898583, 0.27232456, 0.12662607, 0.74165606, 0.43464816, 0.8310301, 0.012846947, 0.9810947, 0.43377626, 0.3608846, 0.22756284, 0.6404164, 0.7243295, 0.68765146, 0.12439847, 0.25675082, 0.26143825, 0.41246158, 0.867953, 0.2895738, 0.3916427, 0.93816304, 0.27819514, 0.1989426, 0.62377095, 0.9969712, 0.4159639, 0.70966166, 0.29150474, 0.6492832, 0.10598481, 0.44674253, 0.03885162, 0.25127923, 0.60202503, 0.6067293, 0.94750637, 0.97315085]]}' \
  localhost:8501/angelServing/v1.0/models/lr/versions/6:predict

curl -H "Content-Type: application/json" -X POST -d '{"instances": [{"sparseIndices":[97,3,4,41,109,16,115,53,117,23,119,27,61],"sparseValues":[0.64,0.36,0.41,0.42,0.20,0.26,0.67,0.11,0.23,0.39,0.16,0.45,0.68]}]}' \
  localhost:8501/angelServing/v1.0/models/lr/versions/6:predict

#pmml model prediction test.
curl -H "Content-Type: application/json" -X POST -d '{"instances": [{"x1":6.2, "x2":2.2, "x3":1.1, "x4":1.}]}' \
  localhost:8501/angelServing/v1.0/models/lr/versions/6:predict

#get prediction summary metric
curl localhost:8501/angelServing/v1.0/monitoring/metrics/summary
