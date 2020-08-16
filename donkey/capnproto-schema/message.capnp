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
    messageId @0 :Int64;
    metaDataId @1 :Int32;
    contentType @2 :CapContentType;
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
    content @3 :Text;
    dataType @4 :Text;
    encrypted @5 :Bool;
}

# default buf size 4 + 8 + 36 + 8 + 256 + 4 + 8 + 8 + 4 + 4 + 4 344B
struct CapConnectorMessage {
    id @0 :Int32;
    messageId @1 :Int64;
    serverId @2 :Text;
    receivedDate @3 :Int64;
    connectorName @4 :Text;
    sendAttempts @5 :Int32;
    sendDate @6 :Int64;
    responseDate @7 :Int64;
    errorCode @8 :Int32;
    chainId @9 :Int32;
    orderId @10 :Int32;
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

# these are the structures for supporting the models present in Server module

struct PreferenceEntry {
  key @0 :Text;
  value @1 :Text;
}

struct CapPerson {
    id               @0 :Int32;
    username         @1 :Text;
    firstname        @2 :Text;
    lastname         @3 :Text;
    organization     @4 :Text;
    industry         @5 :Text;
    email            @6 :Text;
    phonenumber      @7 :Text;
    description      @8 :Text;
    lastLogin        @9 :Int64;
    gracePeriodStart @10 :Int64;
    strikeCount      @11 :Int32;
    lastStrikeTime   @12 :Int64;
    loggedIn         @13 :Int32;
    preferences      @14 :Data;
    password         @15 :Text;
    passwordDate     @16 :Int64;
}

struct CapCodeTemplateLibrary {
    id       @0 :Text;
    name     @1 :Text;
    revision @2 :Int32;
    library  @3 :Text;
}

struct CapChannelGroup {
    id            @0 :Text;
    name          @1 :Text;
    revision      @2 :Int32;
    channelGroup  @3 :Text;
}

struct CapCodeTemplate {
    id            @0 :Text;
    name          @1 :Text;
    revision      @2 :Int32;
    codeTemplate  @3 :Text;
}

struct CapChannel {
    id        @0 :Text;
    name      @1 :Text;
    revision  @2 :Int32;
    channel   @3 :Text;
}

struct CapConfiguration {
    category   @0 :Text;
    props      @1 :Data;
}

struct CapScript {
    groupId  @0 :Text;
    id       @1 :Text;
    script   @2 :Text;
}

struct CapAlert {
    id     @0 :Text;
    name   @1 :Text;
    alert  @2 :Text;
}

struct CapEvent {
    id         @0 :Int32;
    created    @1 :Int64;
    name       @2 :Text;
    level      @3 :Int32;
    outcome    @4 :Int32;
    attributes @5 :Text;
    userId     @6 :Int32;
    ipAddress  @7 :Text;
}