# Orchestration with kestra

## GCP

- service account `de-zoom` with privileges on
    - BQ
    - compute
    - storage
- [upload key with `openSSL`](https://cloud.google.com/iam/docs/keys-upload):

```sh
openssl req -x509 -nodes -newkey rsa:2048 -days 365 \
    -keyout gcp-de-zoom.pem \
    -out gcp-de-zoom-pub.pem \
    -subj "/CN=unused"
```

- this key cannot be used for gcloud cli authentication; use json instead
    - still valid for service-service auth
- `add key` to download the service account json key
- enable [cloud resource manager API]( https://console.developers.google.com/apis/api/cloudresourcemanager.googleapis.com/overview?project=1075001006785)

```sh
docker run -ti -e CLOUDSDK_CONFIG=/config/gcloud \
              -v `pwd`/secrets/gcloud-config:/config/gcloud \
              -v `pwd`/secrets/gcp-key.json:/certs/key.json  asia.gcr.io/google.com/cloudsdktool/google-cloud-cli:508.0.0-stable /bin/bash

# now in bash
# set if there is already existing cred in /config; otherwise don't
gcloud config set auth/credential_file_override /certs/de-zoom-376014.json
# gcp will then persist the cred in CLOUDSDK_CONFIG which we've mounted
gcloud auth activate-service-account --key-file=/certs/key.json
gcloud config set project $GCP_PROJECT_ID # use the unique project ID, not just alias
# verify
gcloud storage buckets list 

# now only need to mount the config dir
docker run -ti -e CLOUDSDK_CONFIG=/config/gcloud \
              -v ~/.config/gcloud-de-zoom:/config/gcloud \
              asia.gcr.io/google.com/cloudsdktool/google-cloud-cli:508.0.0-stable \
              gcloud storage buckets list
```

- add these to `.bash_aliases`

```sh
alias gcloud="docker run -it --rm \
    -v $HOME/.config/gcloud-de-zoom:/root/.config/gcloud \
    asia.gcr.io/google.com/cloudsdktool/google-cloud-cli:508.0.0-stable gcloud"

alias gsutil="docker run -it --rm \
    -v $HOME/.config/gcloud-de-zoom:/root/.config/gcloud \
    asia.gcr.io/google.com/cloudsdktool/google-cloud-cli:508.0.0-stable gsutil"

alias bq="docker run -it --rm \
    -v $HOME/.config/gcloud-de-zoom:/root/.config/gcloud \
    asia.gcr.io/google.com/cloudsdktool/google-cloud-cli:508.0.0-stable bq"
```