#!/usr/bin/python
import os

from lxml import etree
from lxml.html import parse,document_fromstring

from pymongo import Connection
import optparse, csv


cattable = {
    "Premium and Other Important Information": 1,
    "Doctor and Hospital Choice": 2,
	"Inpatient Hospital Care": 3,
    "Inpatient Mental Health Care": 4,
    "Skilled Nursing Facility": 5,
    "Home Health Care": 6,
    "Hospice": 7,
    "Doctor Office Visits": 8,
    "Chiropractic Services": 9,
    "Podiatry Services": 10,
    "Outpatient Mental Health Care": 11,
    "Outpatient Substance Abuse Care": 12,
    "Outpatient Services/Surgery": 13,
    "Ambulance Services": 14,
    "Emergency Care": 15,
    "Urgently Needed Care": 16,
    "Outpatient Rehabilitation Services": 17,
    "Durable Medical Equipment": 18,
    "Prosthetic Devices": 19,
    "Diabetes Self-Monitoring Training and Supplies": 20,
    "Diagnostic Tests, X-Rays, and Lab Services": 21,
    "Bone Mass Measurement": 22,
    "Colorectal Screening Exams": 23,
    "Immunizations": 24,
    "Mammograms (Annual Screening)": 25,
    "Pap Smears and Pelvic Exams": 26,
    "Prostate Cancer Screening Exams": 27,
    "Prescription Drugs": 29,
    "Dental Services": 30,
    "Hearing Services": 31,
    "Vision Services": 32,
    "Physical Exams": 33,
    "Health/Wellness Education": 34,
    "Transportation": 35,
    "Acupuncture": 36,
    "Point of Service":	37,
    "End-Stage Renal Disease (ESRD)": 28 }
    
def fetch(session, contract, plan, segment):
    cmd = u'wget -O - -q --no-cookies --header "Cookie: ASP.NET_SessionId=%s" \'http://www.medicare.gov/find-a-plan/staticpages/plan-details-benefits-popup.aspx?cntrctid=%s&plnid=%s&sgmntid=%s&ctgry=\'' % (session, contract, plan, segment)
    s = os.popen (cmd)
    content = s.read()
    return content
    

def extract_benefit_detail(content):
    root = document_fromstring(content)
    header_list = []
    detail_list = []
    for e in root.cssselect("div.benefitsCategoryHeader"):
        header_list.append(e.text_content().strip())
    for e in root.cssselect("div.benefitsCategoryText"):
        detail_list.append(etree.tostring(e).strip())
    return zip(header_list, detail_list)
    
if __name__ == '__main__':
    p = optparse.OptionParser()
    p.add_option('--verbose', '-v')
    p.add_option('--file', '-f')
    p.add_option('--session', '-s')
    options, arguments = p.parse_args()
    
    
    file_name = options.file
    session = options.session
    connection = Connection('127.0.0.1')
    db = connection['medicare']
    coll = db.benefits
    reader = csv.reader(open(file_name))
    if options.verbose:
        print "read csv file %s" % (file_name,)
    for (contract, plan, segment) in reader:
        planid = "%s-%s-%s" % (contract, plan, segment)
        if options.verbose: 
            print "fetch plan %s" % (planid,)
        content = fetch(session, contract, plan, segment)
        benefits = extract_benefit_detail(content)
        coll.insert({
            "_id": planid
        })
        for (key,value) in benefits:
            if cattable.has_key(key):
                cat_id = cattable[key]
            else:
                cat_id = None
            
            coll.update({ "_id": planid },
                {"$push": { 'details' : {
                        "cat_id": cat_id,
                        "name": key,
                        "value": value
                        }
                    }})
