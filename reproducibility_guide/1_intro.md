# Introduction

The project describes the pre- and post-processing of dMRI data from the [ABCD-BIDS Community Collection (ABCC)](https://docs.abcdstudy.org/v/6_0_0/documentation/imaging/abcc_start_page.html). Release 3.1.0 includes fully processed images from over 24,000 sessions. In this paper we describe the pre- and post-processing of dMRI data. We also harmonize the data, and evaluate which of the derived dMRI measures are most sensitive to development and robust to variation in image quality. Additionally, we manually inspect the quality of a subset of dMRI data, and train a multivariate classifier to predict expert ratings based on automated image quality statistics. The following chapters describe the steps used for each of these processes.

```{warning}
Some of these steps are only meant to be replicated by PennLINC lab members, and some steps are no longer possible due to changes in the central data infrastructure for ABCD. Processing pipelines have also been updated since the data release. However, these steps mainly pertain to data processing, data wrangling, and performing the manual ratings, all things that *do not warrant replication*. Rest assured that the code for the primary analyses are reproducible and will work as long as data are wrangled correctly.
```

```{note}
Please note that access to the data release is limited to researchers with a valid ABCD/HBCD Data Use Certification (https://www.nbdc-datahub.org/).
```

This guide *also* acts as a data narrative, adding details and context that were not appropriate for the publication.

```{note}
For the PennLINC Reproducibilibuddy: please start at the automated classifier QC step in the data munging chapter.
```

## Software

Although you will not be reprocessing this data, note that we used the following software for processing the data:

- [QSIPrep 0.21.4](https://qsiprep.readthedocs.io/en/latest/usage.html) (`singularity build qsiprep_0.21.4.sif docker://pennlinc/qsiprep:0.21.4`)
- [QSIRecon 1.0.0rc2](https://qsirecon.readthedocs.io/en/latest/usage.html) (`singularity build qsirecon_1.0.0rc2.sif docker://pennlinc/qsirecon:1.0.0rc2`)

```{note}
If you are the lab replicator, you can jump to [Replication Setup](7_replication_setup.md).
```

