@0x97534dae5bdc1172;
using Java = import "/capnp/java.capnp";
$Java.package("com.mirth.connect.donkey.model.message");
$Java.outerClassname("CapnpModel");

struct CapAttachment {
    id @0 :Text;
    messageId @1 :Int64;
    type @2 :Text;
    attachmentSize @3 :Int32;
    content @4 :Data;
}

struct CapMessage {
    messageId @0 :Int64;
    serverId @1 :Text;
    channelId @2 :Text;
    receivedDate @3 :Int64;
    processed @4 :Bool;
    originalId @5 :Int64;
    importId @6 :Int64;
    importChannelId @7 :Text;
}

struct CapMessageContent {
    channelId @0 :Text;
    messageId @1 :Int64;
    metaDataId @2 :Int32;
    contentType @3 :CapContentType;
    enum CapContentType {
        raw @0;
        processedraw @1;
        transformed @2;
        encoded @3;
        sent @4;
        response @5;
        responsetransformed @6;
        processedresponse @7;
        connectormap @8;
        channelmap @9;
        responsemap @10;
        processingerror @11;
        postprocessorerror @12;
        responseerror @13;
        sourcemap @14;
    }
    content @4 :Text;
    dataType @5 :Text;
    encrypted @6 :Bool;
}
    
struct CapConnectorMessage {
    id @0 :Int32;
    messageId @1 :Int64;
    serverId @2 :Text;
    receivedDate @3 :Int64;
    status @4 :CapStatus;
    enum CapStatus {
        received @0;
        filtered @1;
        transformed @2;
        sent @3;
        queued @4;
        error @5;
        pending @6;
    }
    connectorName @5 :Text;
    sendAttempts @6 :Int32;
    sendDate @7 :Int64;
    responseDate @8 :Int64;
    errorCode @9 :Int32;
    chainId @10 :Int32;
    orderId @11 :Int32;
 }

struct Map(Key, Value) {
  entries @0 :List(Entry);
  struct Entry {
    key @0 :Key;
    value @1 :Value;
  }
}

struct CapMapContent {
    content @0 :Map(Text, Data);
    encrypted @1 :Bool;
}

struct CapErrorContent {
    content @0 :Text;
    encrypted @1 :Bool;
}

struct CapMetadata {
    metadataId @0 :Int32;
    messageId @1 :Int64;
    columns @2 :List(CapMetadataColumn);
}

struct CapMetadataColumn {
    name @0 :Text;
    type @1 :Type;
    enum Type {
      string @0;
      number @1;
      boolean @2;
      timestamp @3;
    }
    value @2 :Text;
}

struct CapStatistics {
     metadataId @0 :Int32;
     serverId @1 :Text;
     received @2 :Int64;
     receivedLifetime @3 :Int64;
     filtered @4 :Int64;
     filteredLifetime @5 :Int64;
     sent @6 :Int64;
     sentLifetime @7 :Int64;
     error @8 :Int64;
     errorLifetime @9 :Int64;
}