#!/bin/bash

CLI="python mox_util.py cli"
${CLI} ensure-class-exists --bvn "assoc_LR_leader" --title "LR, formand" --facet-bvn "association_type"
${CLI} ensure-class-exists --bvn "assoc_LR" --title "LR" --facet-bvn "association_type"
${CLI} ensure-class-exists --bvn "assoc_FTR_leader" --title "FTR, næstformand" --facet-bvn "association_type"
${CLI} ensure-class-exists --bvn "assoc_FTR" --title "FTR" --facet-bvn "association_type"
${CLI} ensure-class-exists --bvn "assoc_TR_leader" --title "TR, næstformand" --facet-bvn "association_type"
${CLI} ensure-class-exists --bvn "assoc_TR" --title "TR" --facet-bvn "association_type"
${CLI} ensure-class-exists --bvn "assoc_AMR_leader" --title "AMR, næstformand" --facet-bvn "association_type"
${CLI} ensure-class-exists --bvn "assoc_AMR" --title "AMR" --facet-bvn "association_type"
${CLI} ensure-class-exists --bvn "assoc_Medarb_rep_leader" --title "Medarb.rep, næstformand" --facet-bvn "association_type"
${CLI} ensure-class-exists --bvn "assoc_Medarb_rep" --title "Medarb.rep" --facet-bvn "association_type"
${CLI} ensure-class-exists --bvn "assoc_NA" --title "Ej relevant" --facet-bvn "association_type"
