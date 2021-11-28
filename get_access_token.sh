#!/bin/bash

user_id=$1
token_path=$2

curl -X POST https://www.reddit.com/api/v1/access_token -d "grant_type=client_credentials" -u ${user_id} -H "User-Agent: And I, for one, welcome our new insect overlords" | jq -r '.access_token' > ${token_path}
#curl -X POST https://www.reddit.com/api/v1/access_token -d "grant_type=client_credentials" -u xSUq5u8QfBknJxIbgIpm0Q:NdcnfYb0yU8yL5RedxgbEbjgYTKR_Q -H "User-Agent: Fuck Reddit devs" | jq -r '.access_token' > /home/jake/python_workdir/apis/reddit/at.txt
