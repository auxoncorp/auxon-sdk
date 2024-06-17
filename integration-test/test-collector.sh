set -ex

/modality wait-until --deadline 5s 'tick@counter aggregate count() >= 20'
/modality workspace sync-indices
/conform spec eval --file collector.speqtr --dry-run 
