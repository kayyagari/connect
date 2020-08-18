package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
)

var numReq = flag.Int("n", 5000, "number of requests to be sent")
var url = flag.String("u", "http://localhost:8082/je/", "url of the http listener")
func main() {
	flag.Parse()
	fmt.Printf("sending %d requests to %s\n", *numReq, *url)
	msg := []byte(`FHS|^~\&#|SendingAppName^2.4.3.5.2.333.34.1^ISO|NonLabEntity^2.4.3.5.2.222.1.2^ISO|ELRS^2.16.840.1.113883.3.1299.5.1.6.1^ISO|NJDOH^2.16.840.1.113883.3.1299^ISO|20110811014910.0000-0500BHS|^~\&#|SendingAppName^2.4.3.5.2.333.34.1^ISO|NonLabEntity^2.4.3.5.2.222.1.2^ISO|ELRS^2.16.840.1.113883.3.1299.5.1.6.1^ISO|NJDOH^2.16.840.1.113883.3.1299^ISO|20110811014910.0000-0500MSH|^~\&#|SendingAppName^2.4.3.5.2.333.34.1^ISO|LabName^1D32484983^CLIA|Rhapsody^2.16.840.1.113883.3.89.200.1.3.2^ISO|NJDOH^2.16.840.1.113883.3.1299^ISO|20110811014910.0000-0500||ORU^R01^ORU_R01|20110811033501811|P|2.5.1|||||USA||||PHLabReport-Batch^^2.16.840.1.113883.9.11^ISOSFT|Orion Health|3.2|Rhapsody|3.2.0.58783PID|1||16891111060^^^LabName&1.2.3.3.4.6.7&ISO^PI^LabName&1.2.3.3.4.6.7&ISO~123456789^^^SSA&2.16.840.1.113883.3.184&ISO^SS~10-200-3000^^^University Med CtrMPI&2.9.8.7.6.5.4&ISO^PT^University Med Ctr&2.9.8.7.6.5.4&ISO||PATIENTLASTNAME^PATIENTFIRSTNAME^PATIENTMIDDLEINITORNAME||19340925|M||2054-5^Black or African American^CDCREC^B^Black^L^1^^^^^^^2.16.840.1.114222.4.11.836|123 Main Street^Apartment 2^Big City^NJ^00000^USA^H||^NET^Internet^sampleEmail@wxyzdomain.com^^^^^Please note person is hearing impaired|||||||||2186-5^Not Hispanic or Latino^CDCREC^NH^^L^1.1^^^^^^^2.16.840.1.113883.6.238ORC|RE||1B2D0989888^SystemName^1.2.3.4.5.6.7^ISO|||||||||019237311^DRLASTNAME^H^^^^MD^^NPI&2.16.840.1.113883.4.6&ISO^^^^NPI^LabName&1.2.3.4.5.6.7&ISO||^WPN^PH^^1^111^1234567^1~^WPN^PH^^1^111^1234567^24|||||||University Health System^^^^^HL7&2.16.840.1.113883&ISO^XX^University Hospital&2.5.2.3.4.6.432.4&ISO^^1234|350 Boulevard^Suite 3000^SomeCity^NJ^07055|^WPN^PH^^1^111^1234567^1|350 Boulevard^^PASSAIC^NJ^07055^USAOBR|1||1B2D0989888^SystemName^1.2.3.4.5.6.7^ISO|543-9^Mycobacterium sp identified^LN^182675^AFB Cult/Smear, Broth, Suscep^L^2.36^NA^^^^^^2.16.840.1.113883.11.16492|||20110809145706-0500|||||||||019237311^DRLASTNAME^H^^^^MD^^NPI&2.16.840.1.113883.4.6&ISO^^^^NPI^LabName&1.2.3.4.5.6.7&ISO|^WPN^PH^^1^111^1234567^1~^WPN^PH^^1^111^1234567^24|||||201108091457-0500|||F||||||521.00^Unspecified dental caries^I9CDX^^^^1^^^^^^^2.16.840.1.113883.11.15931OBX|1|CWE|6463-4^Bacteria identified^LN^080306^RSLT#3^L^2.36^NA^^^^^^2.16.840.1.113883.11.16492||113861009^Mycobacterium tuberculosis complex^SCT^TBDP^Culture report:^L^5^^^^^^^2.16.840.1.113883.6.5|||A^^HL70078^^^^2.5.1|||F|||20110809145706-0500|||||20110810013504-0500||||LabName^^^^^CLIA&2.16.840.1.113883.4.7&ISO^XX^^^1D32484983|1253 Highway #33^Suite 20^SmallCity^NJ^03977^USA|^DiectorLastName^DirectorFirstName^DirectorMiddleNameSPM|1|^293813&LabName&1.2.3.4.5.6.7&ISO||122554006^Capillary blood specimen^SCT^BLDC^BLDC^L^20080131^^^^^^^2.16.840.1.113883.6.96||||108350001^Abdomen, excluding retroperitoneal region (body structure)^SNM^Abd^Abdomen^L^^^^^^^^2.16.840.1.113883.3.88.12.3221.8.9|||||||||20110809145706-0500|201106172239-0500BTS|1FTS|1`)

	buf := bytes.NewBuffer(msg)
	cl := &http.Client{}
	req, _ := http.NewRequest(http.MethodPost, *url, nil)
	req.Header.Set("content-type", "text/plain")

	start := time.Now()
	var i =0;
	for ; i < *numReq; i++ {
		req.Body = ioutil.NopCloser(buf)
		resp, err := cl.Do(req)
		if err != nil {
			fmt.Printf("%#v\n", err)
			break
		}
		if resp.StatusCode != http.StatusOK {
			fmt.Printf("request %d failed with status code %d reason %s\n", i, resp.StatusCode, resp.Status)
			break;
		}

		resp.Body.Close()
		buf.Reset()
		buf.Write(msg)
	}
	end := time.Now()
	fmt.Printf("time taken to send %d requests %v", i, end.Sub(start))
}
