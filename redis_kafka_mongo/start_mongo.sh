export MONGODB_VERSION=6.0-ubi8
docker run --name mongodb -d -p 27017:27017 \
-e MONGO_INITDB_ROOT_USERNAME=user -e MONGO_INITDB_ROOT_PASSWORD=pass mongodb/mongodb-community-server:$MONGODB_VERSION
# mongosh mongodb://user:pass@127.0.0.1:27017/