### Cloud Run settings

This requires that a Cloud Function be built with an entry point of "ingest_everything" and the contents of this directory added to the function.

In the creation of that function, an environment variable should be set: "GOOGLE_CLOUD_PROJECT" with your project ID.
