@0x97534dae5bdc1172;
using Java = import "/capnp/java.capnp";
$Java.package("com.mirth.connect.donkey.model.message");
$Java.outerClassname("CapnpModel");

struct CapAttachment {
    id @0 :Text;
    content @1 :Data;
    type @2 :Text;
    encrypt @3 :Bool;
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
    messageId @0 :Int64;
    metaDataId @1 :Int32;
    channelId @2 :Text;
    channelName @3 :Text;
    connectorName @4 :Text;
    serverId @5 :Text;
    receivedDate @6 :Int64;
    status @7 :CapStatus;
    enum CapStatus {
        received @0;
        filtered @1;
        transformed @2;
        sent @3;
        queued @4;
        error @5;
        pending @6;
    }
    
    errorCode @8 :Int32;
    sendAttempts @9 :Int32;
    sendDate @10 :Int64;
    responseDate @11 :Int64;
    chainId @12 :Int32;
    orderId @13 :Int32;
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
