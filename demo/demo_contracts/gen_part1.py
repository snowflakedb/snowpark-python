"""Generate contracts 001-005 — professional text-based PDFs."""
import os
from fpdf import FPDF, XPos, YPos

FD = "/opt/miniconda3/envs/sp_test0/lib/python3.13/site-packages/matplotlib/mpl-data/fonts/ttf"
OUT = os.path.dirname(__file__)


class C(FPDF):
    def __init__(self, total) -> None:
        super().__init__()
        self._total = total
        self.add_font("dv", "", f"{FD}/DejaVuSans.ttf")
        self.add_font("dv", "B", f"{FD}/DejaVuSans-Bold.ttf")
        self.add_font("dv", "I", f"{FD}/DejaVuSans-Oblique.ttf")
        self.set_margins(22, 25, 22)
        self.set_auto_page_break(True, 20)

    def footer(self):
        self.set_y(-14)
        self.set_font("dv", "I", 8)
        self.cell(0, 5, f"Page {self.page_no()} of {self._total}", align="R")

    def title_page(self, title, subtitle="", details=None):
        self.add_page()
        self.set_font("dv", "B", 14)
        self.cell(0, 10, title, align="C", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        if subtitle:
            self.set_font("dv", "I", 11)
            self.cell(0, 7, subtitle, align="C", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.ln(4)
        if details:
            self.tbl(["Term", "Detail"], details, [70, 108])
        self.ln(3)

    def sec(self, n, title):
        self.ln(4)
        self.set_font("dv", "B", 11)
        self.multi_cell(
            0, 7, f"{n}.  {title.upper()}", new_x=XPos.LMARGIN, new_y=YPos.NEXT
        )
        self.ln(1)

    def body(self, txt):
        self.set_font("dv", "", 10)
        self.multi_cell(0, 6, txt, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.ln(2)

    def sub(self, n, txt):
        self.set_x(30)
        self.set_font("dv", "", 10)
        w = self.w - self.l_margin - self.r_margin - 8
        self.multi_cell(w, 6, f"{n}  {txt}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.ln(1)

    def tbl(self, cols, rows, widths, gray_hdr=True):
        self.set_font("dv", "B", 9)
        if gray_hdr:
            self.set_fill_color(224, 224, 224)
        for c, w in zip(cols, widths):
            self.cell(w, 7, c, border=1, fill=gray_hdr)
        self.ln()
        self.set_font("dv", "", 9)
        self.set_fill_color(255, 255, 255)
        for row in rows:
            for val, w in zip(row, widths):
                self.cell(w, 6, str(val), border=1)
            self.ln()
        self.ln(2)

    def sig(self, p1="MERKLE INC.", p2="COUNTERPARTY"):
        self.ln(6)
        self.set_font("dv", "B", 10)
        hw = (self.w - self.l_margin - self.r_margin) / 2
        self.cell(hw, 7, p1)
        self.cell(hw, 7, p2, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_font("dv", "", 10)
        for lbl in ["Signature:", "Name:", "Title:", "Date:"]:
            self.cell(hw, 7, f"{lbl} _____________________________")
            self.cell(
                hw,
                7,
                f"{lbl} _____________________________",
                new_x=XPos.LMARGIN,
                new_y=YPos.NEXT,
            )


# ── CONTRACT 001 ─────────────────────────────────────────────────────────────
def c001():
    pdf = C(5)
    pdf.title_page(
        "DATA SERVICES AGREEMENT",
        "Agreement No. DSA-2025-0041",
        [
            ["Agreement Number", "DSA-2025-0041"],
            ["Effective Date", "January 1, 2025"],
            ["Initial Term", "Three (3) years, expiring December 31, 2027"],
            ["Auto-Renewal", "One (1) year unless 90 days prior written notice"],
            ["Annual Minimum Commitment", "USD $360,000"],
            ["Governing Law", "State of Maryland"],
        ],
    )

    pdf.body(
        'This Data Services Agreement ("Agreement") is entered into as of January 1, 2025, '
        "by and between Nexacore Inc., a Delaware corporation with its principal place of business "
        'at 100 Innovation Drive, Boston, MA 02101 ("Client"), and VendorA Analytics LLC, '
        "a Florida limited liability company with its principal place of business at "
        '500 Commerce Blvd, Chicago, IL 60601 ("Provider").'
    )

    pdf.sec(1, "Definitions")
    pdf.body(
        "As used in this Agreement: (a) 'Agreement' means this Data Services Agreement, including "
        "all exhibits and statements of work incorporated herein by reference; (b) 'Client Data' means "
        "any data, information, or materials provided by Client to Provider; (c) 'Confidential Information' "
        "means any non-public information designated as confidential or reasonably understood as such; "
        "(d) 'Deliverables' means the data files and outputs specified in Exhibit A; (e) 'Fees' means "
        "the compensation set forth in Section 3; (f) 'Intellectual Property' means all patents, "
        "copyrights, trademarks, trade secrets, and other proprietary rights; (g) 'SLA' means the "
        "service level agreement governing delivery timeliness and quality; (h) 'Term' means the "
        "Initial Term and any applicable Renewal Terms."
    )

    pdf.sec(2, "Services")
    pdf.sub(
        "2.1",
        "Provider agrees to deliver the Deliverables specified in Exhibit A in accordance with "
        "the delivery schedule, format specifications, and quality standards set forth therein. "
        "Provider shall perform all services in a professional and workmanlike manner consistent with "
        "industry standards and in compliance with all applicable laws and regulations.",
    )
    pdf.sub(
        "2.2",
        "The parties may from time to time execute additional Statements of Work referencing "
        "this Agreement, each of which upon mutual execution shall be incorporated herein by reference. "
        "In the event of a conflict between any Statement of Work and the terms of this Agreement, "
        "the terms of this Agreement shall control unless the Statement of Work expressly states otherwise.",
    )
    pdf.sub(
        "2.3",
        "Provider shall assign qualified personnel to perform the services and shall not "
        "substitute or replace key personnel without providing Client at least fourteen (14) days prior "
        "written notice and receiving Client's written approval, which shall not be unreasonably withheld.",
    )

    pdf.add_page()
    pdf.sec(2, "Services (continued) — Exhibit A: Deliverables Schedule")
    pdf.tbl(
        ["Dataset Name", "Delivery Frequency", "Format", "SLA"],
        [
            [
                "Consumer Segmentation Feed",
                "Monthly",
                "CSV + JSON",
                "5th business day following month",
            ],
            [
                "Identity Linkage File",
                "Bi-weekly",
                "Parquet",
                "3rd business day after period close",
            ],
            [
                "Household Composition Data",
                "Quarterly",
                "CSV",
                "15th calendar day after quarter end",
            ],
            [
                "Digital Propensity Scores",
                "Weekly",
                "JSON",
                "Every Monday by 6:00 AM ET",
            ],
        ],
        [58, 36, 28, 60],
    )

    pdf.sec(3, "Fees and Payment")
    pdf.sub(
        "3.1",
        "Client shall pay Provider an Annual Minimum Commitment of USD $360,000, payable in "
        "equal monthly installments of USD $30,000, due within thirty (30) days of receipt of Provider's "
        "invoice. All invoices shall be submitted electronically to ap@nexacore.com no later "
        "than the fifth (5th) business day of each calendar month.",
    )
    pdf.sub(
        "3.2",
        "In addition to the Annual Minimum Commitment, Client shall pay a per-record fee of "
        "$0.0014 for each record delivered in excess of the volume included in the Annual Minimum. "
        "Per-record overage charges shall be calculated monthly and invoiced within ten (10) business "
        "days of month-end, with payment due thirty (30) days thereafter.",
    )
    pdf.sub(
        "3.3",
        "Amounts not paid within thirty (30) days of the invoice due date shall accrue "
        "interest at the rate of one and one-half percent (1.5%) per month, or the maximum rate "
        "permitted by applicable law, whichever is less. Client shall reimburse Provider for all "
        "reasonable costs of collection, including reasonable attorneys' fees.",
    )

    pdf.sec(4, "Data Delivery and Security")
    pdf.sub(
        "4.1",
        "All Deliverables shall be transmitted to Client via secure SFTP to the designated "
        "Nexacore endpoint or to Client's AWS S3 bucket (s3://nexacore-vendor-ingest/datastream/) as "
        "specified in Exhibit B. Provider shall furnish a manifest file with each delivery identifying "
        "record counts, file checksums (SHA-256), and the delivery timestamp.",
    )
    pdf.sub(
        "4.2",
        "Provider shall maintain SOC 2 Type II certification throughout the Term and shall "
        "provide Client with copies of its annual SOC 2 reports upon written request. Provider shall "
        "implement and maintain commercially reasonable administrative, technical, and physical "
        "safeguards to protect Client Data and Deliverables against unauthorized access or disclosure.",
    )

    pdf.add_page()
    pdf.sec(5, "Intellectual Property")
    pdf.sub(
        "5.1",
        "Client owns all Client Data and any derivatives thereof. Provider shall have no "
        "ownership interest in Client Data and shall use it solely to perform its obligations "
        "hereunder. Provider retains ownership of all pre-existing intellectual property and of any "
        "general methodologies, tools, or know-how developed independently of this Agreement.",
    )
    pdf.sub(
        "5.2",
        "Provider hereby grants Client a non-exclusive, non-transferable license to use the "
        "Deliverables for Client's internal business purposes and for providing data analytics "
        "services to Client's direct advertising clients, subject to the restrictions set forth herein.",
    )

    pdf.sec(6, "Confidentiality")
    pdf.sub(
        "6.1",
        "Each party agrees to hold in strict confidence all Confidential Information of the "
        "other party, to use such information solely for the purposes of this Agreement, and to "
        "disclose it only to employees or contractors with a need to know and who are bound by "
        "confidentiality obligations no less restrictive than those herein.",
    )
    pdf.sub(
        "6.2",
        "The confidentiality obligations set forth in this Section shall survive the "
        "expiration or termination of this Agreement for a period of two (2) years. Neither party "
        "shall be obligated to maintain confidentiality with respect to information that: (a) is or "
        "becomes publicly available without breach of this Agreement; (b) was independently developed "
        "without reference to the disclosing party's information; or (c) is required to be disclosed "
        "by law or court order, provided the receiving party gives prompt prior written notice.",
    )

    pdf.sec(7, "Representations and Warranties")
    pdf.sub(
        "7.1",
        "Provider represents and warrants that: (a) it has the full right, power, and authority "
        "to enter into this Agreement and to grant the licenses herein; (b) the Deliverables do not "
        "infringe the intellectual property rights of any third party; (c) all data included in the "
        "Deliverables was collected, processed, and transferred in compliance with all applicable "
        "privacy laws, including the California Consumer Privacy Act (CCPA) and applicable GDPR "
        "requirements; and (d) the Deliverables will conform to the specifications set forth in Exhibit A.",
    )
    pdf.sub(
        "7.2",
        "Client represents and warrants that: (a) it has the full right, power, and authority "
        "to enter into this Agreement; (b) it will use the Deliverables solely in accordance with the "
        "license granted herein and all applicable laws; and (c) it will maintain appropriate security "
        "controls to protect the Deliverables from unauthorized access or disclosure.",
    )

    pdf.add_page()
    pdf.sec(8, "Indemnification")
    pdf.sub(
        "8.1",
        'Each party ("Indemnifying Party") shall defend, indemnify, and hold harmless the '
        'other party and its officers, directors, employees, and agents ("Indemnified Party") from '
        "and against any third-party claims, losses, damages, liabilities, and expenses (including "
        "reasonable attorneys' fees) arising from: (a) the Indemnifying Party's material breach of "
        "this Agreement; (b) the Indemnifying Party's negligence or willful misconduct; or "
        "(c) in the case of Provider, any claim that the Deliverables infringe any third-party "
        "intellectual property rights.",
    )
    pdf.sub(
        "8.2",
        "The Indemnified Party shall: (a) promptly notify the Indemnifying Party in writing "
        "of any claim; (b) give the Indemnifying Party sole control of the defense and settlement; "
        "and (c) provide reasonable cooperation. The Indemnifying Party shall not settle any claim "
        "that imposes obligations on the Indemnified Party without its prior written consent.",
    )

    pdf.sec(9, "Limitation of Liability")
    pdf.sub(
        "9.1",
        "NEITHER PARTY SHALL BE LIABLE TO THE OTHER FOR ANY INDIRECT, INCIDENTAL, SPECIAL, "
        "CONSEQUENTIAL, EXEMPLARY, OR PUNITIVE DAMAGES, INCLUDING LOST PROFITS OR DATA, ARISING OUT "
        "OF OR RELATED TO THIS AGREEMENT, REGARDLESS OF THE FORM OF ACTION AND WHETHER SUCH DAMAGES "
        "WERE FORESEEABLE OR SUCH PARTY WAS ADVISED OF THEIR POSSIBILITY.",
    )
    pdf.sub(
        "9.2",
        "EACH PARTY'S AGGREGATE LIABILITY ARISING OUT OF OR RELATED TO THIS AGREEMENT SHALL "
        "NOT EXCEED THE TOTAL FEES PAID OR PAYABLE BY CLIENT TO PROVIDER DURING THE TWELVE (12) "
        "MONTH PERIOD IMMEDIATELY PRECEDING THE EVENT GIVING RISE TO THE CLAIM.",
    )
    pdf.sub(
        "9.3",
        "The limitations in Sections 9.1 and 9.2 shall not apply to: (a) either party's "
        "indemnification obligations under Section 8; (b) either party's breach of Section 6 "
        "(Confidentiality); or (c) fraud or willful misconduct.",
    )

    pdf.sec(10, "Term and Termination")
    pdf.sub(
        "10.1",
        "This Agreement shall commence on the Effective Date and continue for the Initial "
        "Term of three (3) years. Thereafter, this Agreement shall automatically renew for successive "
        "one-year terms unless either party provides at least ninety (90) days prior written notice "
        "of non-renewal before the end of the then-current term.",
    )
    pdf.sub(
        "10.2",
        "Either party may terminate this Agreement for convenience upon one hundred eighty "
        "(180) days prior written notice to the other party. Client shall pay all undisputed fees "
        "accrued through the effective date of termination.",
    )
    pdf.sub(
        "10.3",
        "Either party may terminate this Agreement for cause if the other party materially "
        "breaches this Agreement and fails to cure such breach within thirty (30) days of written "
        "notice describing the breach in reasonable detail.",
    )
    pdf.sub(
        "10.4",
        "Upon termination or expiration: (a) each party shall promptly return or destroy "
        "the other party's Confidential Information; (b) Provider shall deliver a final data extract "
        "within thirty (30) days; and (c) Client shall pay all undisputed amounts owed.",
    )

    pdf.add_page()
    pdf.sec(11, "General Provisions")
    pdf.sub(
        "11.1",
        "Notices. All notices under this Agreement shall be in writing and delivered by: "
        "(a) overnight courier to Nexacore Inc., Attention: General Counsel, 7001 Columbia Gateway "
        "Drive, Columbia, MD 21046; or to VendorA Analytics LLC, Attention: Legal Department, "
        "500 Commerce Blvd, Chicago, IL 60601; or (b) email to legal@nexacore.com or "
        "legal@datastreamanalytics.com with confirmation of receipt.",
    )
    pdf.sub(
        "11.2",
        "Assignment. Neither party may assign this Agreement or any rights or obligations "
        "hereunder without the other party's prior written consent, which shall not be unreasonably "
        "withheld. Notwithstanding the foregoing, either party may assign this Agreement without "
        "consent in connection with a merger, acquisition, or sale of all or substantially all of "
        "its assets, provided the assignee assumes all obligations hereunder.",
    )
    pdf.sub(
        "11.3",
        "Entire Agreement; Amendment. This Agreement, including all exhibits, constitutes "
        "the entire agreement between the parties with respect to the subject matter hereof and "
        "supersedes all prior and contemporaneous agreements, representations, and understandings. "
        "This Agreement may only be amended by a written instrument signed by both parties.",
    )
    pdf.sub(
        "11.4",
        "Force Majeure. Neither party shall be liable for delay or failure to perform "
        "resulting from causes beyond its reasonable control, including acts of God, natural disasters, "
        "pandemics, governmental actions, or internet or telecommunications failures, provided the "
        "affected party gives prompt written notice and uses reasonable efforts to mitigate the impact.",
    )
    pdf.sub(
        "11.5",
        "Counterparts. This Agreement may be executed in one or more counterparts, each of "
        "which shall be an original and all of which together shall constitute one instrument. "
        "Electronic signatures shall be deemed valid and binding to the same extent as original signatures.",
    )
    pdf.sub(
        "11.6",
        "Governing Law; Disputes. This Agreement shall be governed by the laws of the State "
        "of Maryland, without regard to conflict of law principles. Any dispute shall first be subject "
        "to good-faith negotiation for thirty (30) days, after which either party may pursue its "
        "remedies in courts of competent jurisdiction in the State of Maryland.",
    )
    pdf.ln(4)
    pdf.sig("FOR MERKLE INC.", "FOR DATASTREAM ANALYTICS LLC")
    pdf.output(f"{OUT}/contract_001_data_services_agreement.pdf")
    print("Written: contract_001_data_services_agreement.pdf")


# ── CONTRACT 002 ─────────────────────────────────────────────────────────────
def c002():
    pdf = C(4)
    pdf.title_page(
        "CLOUD INFRASTRUCTURE SERVICES RENEWAL AGREEMENT",
        "Agreement No. CVS-2022-0441-R1 | Renewal Term: April 1, 2025 – March 31, 2027",
    )

    pdf.body(
        "RECITALS\n\n"
        'WHEREAS, Nexacore Inc. ("Customer") and VendorB Corp. ("Provider") entered into that '
        'certain Cloud Infrastructure Services Agreement dated March 15, 2022 ("Original Agreement"), '
        "Agreement No. CVS-2022-0441, for the provision of cloud infrastructure and managed services;\n\n"
        "WHEREAS, the Original Agreement is scheduled to expire on March 31, 2025, and both parties "
        "have performed their respective obligations thereunder in a satisfactory manner and desire "
        "to continue their business relationship;\n\n"
        "WHEREAS, the parties desire to renew the Original Agreement for a further period on the "
        "modified terms and conditions set forth herein;\n\n"
        "NOW, THEREFORE, in consideration of the mutual covenants contained herein, and for other "
        "good and valuable consideration, the receipt and sufficiency of which are hereby acknowledged, "
        "the parties agree as follows:"
    )

    pdf.sec(1, "Renewal of Agreement")
    pdf.body(
        "Customer and Provider hereby renew the Original Agreement for a period of twenty-four (24) "
        'months commencing April 1, 2025 and expiring March 31, 2027 (the "Renewal Term"). Except '
        "as expressly modified by this Renewal Agreement, all terms and conditions of the Original "
        "Agreement shall remain in full force and effect during the Renewal Term and are incorporated "
        "herein by reference."
    )

    pdf.sec(2, "Modified Service Terms")
    pdf.sub(
        "2.1",
        "Effective April 1, 2025, the Annual Service Fees shall be increased by eight percent "
        "(8%) above the fees applicable during the final year of the Original Agreement, as set forth "
        "in the table below. Provider shall invoice Customer monthly in equal installments of the "
        "applicable annual fee divided by twelve (12).",
    )
    pdf.tbl(
        ["Service Tier", "2022-2025 Annual Fee", "2025-2027 Annual Fee", "Increase"],
        [
            ["Enterprise", "$580,000", "$626,400", "8.0%"],
            ["Professional", "$310,000", "$334,800", "8.0%"],
            ["Starter", "$95,000", "$102,600", "8.0%"],
        ],
        [52, 52, 52, 26],
    )
    pdf.sub(
        "2.2",
        "Provider shall provide Customer with at least ninety (90) days prior written notice "
        "before implementing any additional pricing changes during the Renewal Term. No fee increases "
        "beyond those set forth in Section 2.1 shall take effect without Customer's prior written consent.",
    )

    pdf.add_page()
    pdf.sec(3, "Service Level Commitments")
    pdf.sub(
        "3.1",
        "During the Renewal Term, Provider shall maintain the following service levels. "
        "Failure to meet the applicable service level commitment shall entitle Customer to service "
        "credits as specified, which shall be Customer's sole and exclusive remedy for such failure.",
    )
    pdf.tbl(
        [
            "Service Tier",
            "Uptime Guarantee",
            "Incident Response SLA",
            "Monthly Credit (Non-Compliance)",
        ],
        [
            ["Platinum", "99.99%", "15 minutes", "30% of monthly fee"],
            ["Gold", "99.95%", "1 hour", "20% of monthly fee"],
            ["Silver", "99.90%", "4 hours", "10% of monthly fee"],
            ["Bronze", "99.50%", "8 hours", "5% of monthly fee"],
        ],
        [30, 28, 38, 86],
    )
    pdf.sub(
        "3.2",
        "Uptime is measured on a calendar-month basis excluding scheduled maintenance windows "
        "of which Provider has given at least seventy-two (72) hours advance notice. Provider shall "
        "publish a maintenance calendar at least thirty (30) days in advance of each calendar month "
        "and shall make reasonable efforts to schedule maintenance during off-peak hours.",
    )

    pdf.sec(4, "Billing and Payment")
    pdf.body(
        "Invoices are due Net-30 from date of receipt. Customer's preferred payment method is ACH "
        "electronic transfer; Provider shall provide banking instructions upon request. Amounts "
        "outstanding beyond thirty (30) days from the due date shall accrue interest at one and "
        "one-half percent (1.5%) per month. Customer shall notify Provider in writing of any invoice "
        "dispute within fifteen (15) days of receipt, and the parties shall work in good faith to "
        "resolve any such dispute within thirty (30) days."
    )

    pdf.sec(5, "Modifications to Prior Agreement")
    pdf.body(
        "The following provisions of the Original Agreement are hereby modified for the Renewal Term:\n"
        "(a) Security Exhibit: Exhibit C (Security Requirements) is replaced in its entirety with "
        "the updated Exhibit C-1 attached hereto, incorporating SOC 2 Type II, ISO 27001, and "
        "PCI-DSS compliance obligations;\n"
        "(b) Escalation Procedure: Section 12.4 (Escalation) is amended to reduce initial response "
        "time from 4 hours to 2 hours for Priority 1 incidents, and to add a C-suite escalation "
        "path for incidents unresolved after 8 hours;\n"
        "(c) Backup Retention: Section 8.2 (Data Backup) is amended to extend backup retention "
        "from 30 days to 90 days for all Customer data, with no additional fee."
    )

    pdf.sec(6, "Representations and Warranties")
    pdf.body(
        "Each party represents and warrants that: (a) it has the full legal power and authority "
        "to enter into this Renewal Agreement; (b) the execution hereof does not conflict with "
        "any obligation to a third party; and (c) it will perform its obligations in compliance "
        "with all applicable laws and regulations. Provider additionally represents that it has "
        "not experienced any material security incidents affecting customer data in the twelve "
        "(12) months preceding execution of this Renewal Agreement."
    )

    pdf.add_page()
    pdf.sec(7, "General Provisions")
    pdf.body(
        "This Renewal Agreement shall be governed by the laws of the State of California. "
        "All other general provisions of the Original Agreement, including but not limited to "
        "confidentiality, indemnification, limitation of liability, dispute resolution, and "
        "assignment restrictions, shall continue to apply during the Renewal Term unchanged. "
        "This Renewal Agreement, together with the Original Agreement and its exhibits as modified "
        "herein, constitutes the entire agreement of the parties with respect to the subject matter "
        "hereof and supersedes all prior negotiations, representations, or agreements relating to "
        "the renewal of the Original Agreement."
    )
    pdf.sig('FOR MERKLE INC. ("Customer")', 'FOR CLOUDVAULT CORP. ("Provider")')
    pdf.add_page()
    pdf.body("(This page intentionally left blank.)")
    pdf.output(f"{OUT}/contract_002_cloud_services_renewal.pdf")
    print("Written: contract_002_cloud_services_renewal.pdf")


# ── CONTRACT 003 ─────────────────────────────────────────────────────────────
def c003():
    pdf = C(2)
    pdf.title_page(
        "ADDENDUM NO. 1 TO DATA DELIVERY AGREEMENT",
        "Data Schema Modification and Transition Terms",
        [
            ["Addendum Reference", "Addendum No. 1 to Agreement NXC-DA-2023-0077"],
            ["Parties", "Nexacore Inc. and VendorC Partners Inc."],
            ["Effective Date", "February 15, 2025"],
            ["Dataset Affected", "Consumer Identity Feed (Dataset ID: CIF-001)"],
        ],
    )

    pdf.body(
        'WHEREAS, Nexacore Inc. ("Client") and VendorC Partners Inc. ("Provider") entered '
        'into the Data Delivery Agreement No. NXC-DA-2023-0077 dated April 1, 2023 (the "Agreement"); '
        "WHEREAS, Client's updated identity resolution platform requires additions to the data schema "
        "of the Consumer Identity Feed; WHEREAS, both parties desire to formalize the schema changes "
        "and establish a transition period; NOW THEREFORE, the parties agree as follows:"
    )

    pdf.sec(1, "Purpose and Scope")
    pdf.body(
        "This Addendum modifies the data schema of the Consumer Identity Feed delivered by Provider "
        "to Client pursuant to the Agreement. The new fields described herein are required to support "
        "Nexacore's identity resolution platform v4.0 and to align with current industry taxonomy "
        "standards. All other terms of the Agreement remain unchanged."
    )

    pdf.sec(2, "New Data Fields")
    pdf.body(
        "Effective on the Addendum Effective Date, the Consumer Identity Feed shall include the "
        "following new fields appended to the existing schema record layout:"
    )
    pdf.tbl(
        ["Field Name", "Data Type", "Max Length", "Description", "Nullable"],
        [
            [
                "consumer_segment_v2",
                "VARCHAR",
                "50",
                "Updated segmentation taxonomy v2.0",
                "NOT NULL",
            ],
            [
                "household_income_band",
                "INTEGER",
                "—",
                "Income band code 1–10 per census tract",
                "NULL",
            ],
            [
                "digital_propensity_score",
                "FLOAT",
                "—",
                "0.0–1.0 digital engagement propensity",
                "NULL",
            ],
            [
                "email_hash_sha256",
                "CHAR",
                "64",
                "SHA-256 hash of lowercase email address",
                "NULL",
            ],
            [
                "device_fingerprint_id",
                "UUID",
                "36",
                "Deterministic cross-device identifier",
                "NULL",
            ],
            [
                "last_purchase_channel",
                "VARCHAR",
                "30",
                "e-commerce, in-store, mobile, catalog",
                "NULL",
            ],
            [
                "loyalty_tier",
                "SMALLINT",
                "—",
                "0=None 1=Silver 2=Gold 3=Platinum",
                "NULL",
            ],
            [
                "verified_address_flag",
                "BOOLEAN",
                "—",
                "TRUE if USPS CASS-certified",
                "NOT NULL",
            ],
        ],
        [40, 20, 20, 68, 24],
    )

    pdf.sec(3, "Deprecated Fields")
    pdf.body(
        "The following fields shall be deprecated effective ninety (90) days after the Addendum "
        'Effective Date (the "Deprecation Date"):\n'
        "  (a) legacy_consumer_id (VARCHAR) — replaced by consumer_segment_v2;\n"
        "  (b) zip4_extended (CHAR) — superseded by verified_address_flag logic;\n"
        "  (c) segment_code_v1 (CHAR) — replaced by consumer_segment_v2.\n"
        "Provider shall continue to populate deprecated fields through the Deprecation Date. "
        "Client is responsible for updating downstream processes prior to the Deprecation Date."
    )

    pdf.add_page()
    pdf.sec(4, "Transition Period")
    pdf.body(
        'For a period of sixty (60) days following the Addendum Effective Date (the "Transition '
        'Period"), Provider shall deliver both the legacy schema and the new schema in parallel, '
        "as separate files with the suffix '_v1' and '_v2' respectively. Client shall notify "
        "Provider in writing when it has successfully migrated all downstream systems, at which "
        "point Provider may discontinue parallel delivery of the legacy schema file."
    )

    pdf.sec(5, "Technical Specifications")
    pdf.body(
        "All new schema fields shall be delivered in Parquet version 2 format with Snappy compression, "
        "partitioned by delivery_date in YYYY-MM-DD format. Provider shall update the data dictionary "
        "documentation in Exhibit B of the Agreement within fifteen (15) days of the Addendum "
        "Effective Date. Provider shall provide Client with sample records containing the new fields "
        "at least ten (10) business days before the Addendum Effective Date for validation testing."
    )

    pdf.sec(6, "No Other Modifications")
    pdf.body(
        "Except as expressly set forth in this Addendum, all terms and conditions of the Agreement "
        "shall remain in full force and effect and are hereby ratified and confirmed. In the event "
        "of any inconsistency between this Addendum and the Agreement, the terms of this Addendum "
        "shall control."
    )

    pdf.sec(7, "Counterparts")
    pdf.body(
        "This Addendum may be executed in one or more counterparts, each of which shall be deemed "
        "an original and all of which together shall constitute one and the same instrument. "
        "Electronic signatures shall have the same legal effect as original handwritten signatures."
    )
    pdf.ln(4)
    pdf.sig("FOR MERKLE INC.", "FOR PRECISIONDATA PARTNERS INC.")
    pdf.output(f"{OUT}/contract_003_schema_addendum.pdf")
    print("Written: contract_003_schema_addendum.pdf")


# ── CONTRACT 004 ─────────────────────────────────────────────────────────────
def c004():
    pdf = C(3)
    pdf.title_page(
        "AMENDMENT NO. 2 TO DATA SERVICES AGREEMENT",
        "Revised Financial Terms and Pricing Schedule",
        [
            ["Amendment Reference", "Amendment No. 2 to Agreement NXC-DB-2022-0134"],
            ["Parties", "Nexacore Inc. and VendorD Solutions Inc."],
            ["Effective Date", "July 1, 2025"],
            ["Original Agreement Date", "January 15, 2022"],
        ],
    )

    pdf.body(
        'WHEREAS, Nexacore Inc. ("Client") and VendorD Solutions Inc. ("Provider") entered '
        "into the Data Services Agreement No. NXC-DB-2022-0134 dated January 15, 2022, and Amendment "
        'No. 1 thereto dated January 10, 2023 (collectively, the "Agreement"); WHEREAS, annual '
        "data volumes have significantly exceeded initial projections, reflecting the growing strategic "
        "importance of Provider's data assets to Client's platform; WHEREAS, the parties desire to "
        "establish a revised pricing framework that reflects current volume levels and provides "
        "predictable economics for both parties through the remainder of the contract term; NOW "
        "THEREFORE, the parties agree as follows:"
    )

    pdf.sec(1, "Background and Purpose")
    pdf.body(
        "This Amendment No. 2 is entered into to revise the financial terms of the Agreement "
        "effective July 1, 2025. The revised terms reflect a mutual acknowledgment that actual "
        "data volumes have grown substantially beyond the baseline assumptions underlying the "
        "original pricing structure. The parties have negotiated these revised terms in good faith "
        "with the objective of maintaining a commercially equitable and sustainable arrangement "
        "through the expiration of the Agreement on December 31, 2027."
    )

    pdf.sec(2, "Revised Annual Minimum Commitments")
    pdf.sub(
        "2.1",
        "Effective as of the Effective Date, Section 3.1 (Annual Minimum Commitment) of the "
        "Agreement is hereby amended and restated in its entirety as follows:",
    )
    pdf.tbl(
        [
            "Contract Year",
            "Contract Period",
            "Annual Minimum Commitment",
            "Per-Record Base Rate",
        ],
        [
            [
                "Year 3",
                "January 1, 2025 – December 31, 2025",
                "$480,000",
                "$0.0016/record",
            ],
            [
                "Year 4",
                "January 1, 2026 – December 31, 2026",
                "$520,000",
                "$0.0015/record",
            ],
            [
                "Year 5",
                "January 1, 2027 – December 31, 2027",
                "$560,000",
                "$0.0014/record",
            ],
        ],
        [22, 68, 54, 38],
    )
    pdf.sub(
        "2.2",
        "The Annual Minimum Commitment shall be payable in equal monthly installments. "
        "To the extent that cumulative record deliveries in any Contract Year do not meet the "
        "volume threshold implied by the Annual Minimum, Client shall pay a year-end true-up "
        "equal to the Annual Minimum less the total fees actually invoiced, due within thirty "
        "(30) days of Provider's issuance of an annual reconciliation statement.",
    )

    pdf.add_page()
    pdf.sec(3, "Volume Discount Schedule")
    pdf.sub(
        "3.1",
        "For records delivered in excess of the volume included in the Annual Minimum "
        "Commitment, the following volume discount schedule shall apply, calculated on a cumulative "
        "annual basis and applied to the Per-Record Base Rate for the applicable Contract Year:",
    )
    pdf.tbl(
        ["Annual Record Volume Threshold", "Discount Applied", "Notes"],
        [
            [
                "Below 50 million records",
                "0% (base rate)",
                "Standard per-record base rate applies",
            ],
            [
                "50 million – 100 million records",
                "10% discount",
                "Applied to overage records in this tier",
            ],
            [
                "100 million – 200 million records",
                "20% discount",
                "Applied to overage records in this tier",
            ],
            [
                "Above 200 million records",
                "30% discount",
                "Applied to all records exceeding 200M threshold",
            ],
        ],
        [60, 34, 88],
    )
    pdf.sub(
        "3.2",
        "Volume discounts shall be calculated quarterly and applied as a credit against the "
        "following month's invoice. Provider shall provide a quarterly volume report within ten "
        "(10) business days of each quarter's end, showing cumulative record counts, applicable "
        "tiers, and the resulting discount credits.",
    )

    pdf.sec(4, "Payment Terms")
    pdf.body(
        "All invoices shall be issued on a monthly basis and are due Net-30 from date of receipt. "
        "Client's preferred payment method is ACH electronic transfer. An annual reconciliation "
        "statement shall be issued within thirty (30) days of each Contract Year-end. Client "
        "shall notify Provider in writing of any invoice dispute within fifteen (15) business days "
        "of receipt, and the parties shall resolve any bona fide dispute within thirty (30) days "
        "through good-faith negotiation. Undisputed portions of invoices remain due on the normal "
        "payment schedule."
    )

    pdf.sec(5, "Late Fees and Remedies")
    pdf.body(
        "Amounts outstanding beyond forty-five (45) days from the invoice due date shall accrue "
        "interest at the rate of one and one-half percent (1.5%) per month. If Client's account "
        "remains delinquent beyond sixty (60) days, Provider may, upon ten (10) days prior written "
        "notice, suspend data deliveries until the delinquent balance is cured. Provider's right "
        "to suspend is in addition to, and not in lieu of, any other remedies available at law "
        "or equity."
    )

    pdf.add_page()
    pdf.sec(6, "Price Escalation Protection")
    pdf.body(
        "The Per-Record Base Rates set forth in Section 2 are fixed for Contract Years 3 through 5. "
        "Provider may not increase pricing for Years 4 and 5 beyond the rates specified in the "
        "table in Section 2.1, except that Provider may propose CPI-based adjustments for Year 5 "
        "subject to a maximum increase of four percent (4%) over the Year 4 rate. Any such CPI "
        "adjustment proposal must be submitted to Client in writing no later than ninety (90) days "
        "before the commencement of Contract Year 5, and shall become effective only upon Client's "
        "written acceptance."
    )

    pdf.sec(7, "Disputed Invoices")
    pdf.body(
        "If Client disputes any portion of an invoice in good faith, Client shall: (a) pay "
        "all undisputed amounts by the due date; (b) provide written notice to Provider within "
        "fifteen (15) business days specifying the nature and amount of the dispute; and "
        "(c) cooperate in good faith with Provider to resolve the dispute within thirty (30) days. "
        "If the parties are unable to resolve a dispute within thirty (30) days, the matter shall "
        "be escalated to senior management of each party for resolution within an additional fifteen "
        "(15) days, failing which either party may pursue its remedies under the Agreement."
    )

    pdf.sec(8, "Remaining Terms Unchanged")
    pdf.body(
        "Except as expressly set forth in this Amendment No. 2, all terms and conditions of the "
        "Agreement, including Amendment No. 1, shall remain in full force and effect and are "
        "hereby ratified and confirmed in their entirety. This Amendment No. 2, together with "
        "the Agreement and Amendment No. 1, constitutes the entire agreement of the parties with "
        "respect to the subject matter hereof."
    )
    pdf.ln(4)
    pdf.sig("FOR MERKLE INC.", "FOR DATABRIDGE SOLUTIONS INC.")
    pdf.output(f"{OUT}/contract_004_financial_terms_amendment.pdf")
    print("Written: contract_004_financial_terms_amendment.pdf")


# ── CONTRACT 005 ─────────────────────────────────────────────────────────────
def c005():
    pdf = C(4)
    pdf.title_page(
        "DATA LICENSE AND PERMITTED USE AGREEMENT",
        "Agreement No. DLPUA-2025-0088",
        [
            ["Agreement Number", "DLPUA-2025-0088"],
            ["Licensee", "Nexacore Inc., 100 Innovation Drive, Boston, MA 02101"],
            [
                "Licensor",
                "VendorE Solutions Inc., 300 Technology Way, Austin, TX 78701",
            ],
            ["Effective Date", "March 1, 2025"],
            ["Annual License Fee", "USD $275,000"],
            ["Initial Term", "One (1) year, auto-renewing"],
        ],
    )

    pdf.body(
        "NOW, THEREFORE, in consideration of the mutual covenants and agreements contained herein, "
        "the receipt and sufficiency of which are hereby acknowledged, the parties agree as follows:"
    )

    pdf.sec(1, "Definitions")
    pdf.body(
        "(a) 'Licensed Data' means the consumer identity, demographic, behavioral, and household "
        "data files made available by Licensor to Licensee pursuant to this Agreement, as described "
        "in Exhibit A; (b) 'Permitted Purposes' means the specific uses of the Licensed Data "
        "authorized under Section 2; (c) 'Derivative Work' means any work that is based upon or "
        "derived from the Licensed Data; (d) 'Licensee Clients' means advertising and marketing "
        "clients that have contracted directly with Nexacore Inc. for data analytics or media "
        "activation services; (e) 'Prohibited Uses' means the activities described in Section 3."
    )

    pdf.sec(2, "Grant of License")
    pdf.body(
        "Subject to the terms of this Agreement and Licensee's payment of the License Fee, "
        "Licensor hereby grants to Licensee a non-exclusive, non-transferable, limited license "
        "to access and use the Licensed Data solely for the following Permitted Purposes:"
    )
    pdf.body(
        "(a) Development, enhancement, and maintenance of Licensee's proprietary consumer identity "
        "resolution graph and related data products, provided that such products are used solely "
        "for Licensee's internal purposes or for providing services to Licensee's direct clients;\n"
        "(b) Audience segmentation, targeting, and activation for advertising campaigns conducted "
        "by or on behalf of Licensee Clients, limited to the following verticals: financial services, "
        "retail and e-commerce, consumer packaged goods, telecommunications, and media & entertainment;\n"
        "(c) Attribution measurement, closed-loop reporting, and campaign effectiveness analytics "
        "provided to Licensee Clients as part of Licensee's managed services offering;\n"
        "(d) Household-level statistical modeling, propensity scoring, and predictive analytics for "
        "internal product development, provided no individual-level Licensed Data is exposed externally;\n"
        "(e) First-party data onboarding, identity matching, and audience extension services "
        "performed by Licensee on behalf of its direct clients, subject to data governance "
        "requirements in Section 4."
    )

    pdf.add_page()
    pdf.sec(3, "Restrictions and Prohibited Uses")
    pdf.body(
        "Notwithstanding Section 2, Licensee shall not, and shall ensure its personnel do not:\n"
        "(a) Resell, sublicense, transfer, or otherwise make available the Licensed Data or any "
        "Derivative Work in raw or substantially unmodified form to any third party without prior "
        "written consent of Licensor;\n"
        "(b) Use the Licensed Data in connection with any product, service, or business activity "
        "that directly competes with Licensor's core identity data business, including consumer "
        "identity resolution, data brokerage, or identity graph licensing;\n"
        "(c) Disclose or provide access to the Licensed Data to any affiliate or subsidiary not "
        "party to this Agreement without executing a separate written agreement with Licensor;\n"
        "(d) Use the Licensed Data for any government contracting, law enforcement, credit "
        "underwriting, insurance underwriting, or employment screening purpose;\n"
        "(e) Process, store, or transfer the Licensed Data outside of the United States and "
        "Canada without prior written consent of Licensor;\n"
        "(f) Combine the Licensed Data with sensitive personal data categories including health, "
        "financial account, or biometric data without obtaining prior written approval."
    )

    pdf.sec(4, "Sublicensing and Client Activation")
    pdf.sub(
        "4.1",
        "Licensee may use the Licensed Data to provide aggregated, anonymized audience "
        "segments to Licensee Clients for media activation purposes. Licensee shall not provide "
        "any individual-level records from the Licensed Data to Licensee Clients under any "
        "circumstances. All client-facing outputs derived from the Licensed Data must be at "
        "an aggregated audience segment level with a minimum segment size of five thousand (5,000) "
        "individuals.",
    )
    pdf.sub(
        "4.2",
        "Each Licensee Client receiving services derived from the Licensed Data must be "
        "bound by a written agreement with Licensee that includes data protection obligations "
        "no less restrictive than those in this Agreement. Licensee shall maintain records of "
        "all Licensee Clients receiving such services and shall provide such records to Licensor "
        "upon written request.",
    )

    pdf.add_page()
    pdf.sec(5, "Audit Rights")
    pdf.sub(
        "5.1",
        "Licensor shall have the right, upon thirty (30) days prior written notice and "
        "no more than once per calendar year, to audit Licensee's use of the Licensed Data to "
        "verify compliance with this Agreement. Such audits shall be conducted during Licensee's "
        "normal business hours and shall be subject to reasonable confidentiality protections.",
    )
    pdf.sub(
        "5.2",
        "Licensee shall bear the cost of any audit that reveals a material non-compliance. "
        "Licensor shall bear all audit costs in all other cases. 'Material non-compliance' means "
        "a violation that results in or is reasonably likely to result in financial harm to Licensor "
        "or unauthorized disclosure of Licensed Data.",
    )

    pdf.sec(6, "Intellectual Property Ownership")
    pdf.body(
        "All rights, title, and interest in and to the Licensed Data, including all intellectual "
        "property rights therein, are and shall remain the sole property of Licensor. This "
        "Agreement grants Licensee no ownership interest in the Licensed Data. Licensee "
        "acknowledges that the Licensed Data constitutes valuable proprietary information of "
        "Licensor and agrees to take reasonable steps to protect it from unauthorized disclosure."
    )

    pdf.sec(7, "Confidentiality")
    pdf.body(
        "The Licensed Data and the terms of this Agreement shall be treated as Confidential "
        "Information of Licensor. Licensee's confidentiality obligations shall survive termination "
        "or expiration of this Agreement for a period of three (3) years. Licensor's confidentiality "
        "obligations with respect to Licensee's Confidential Information shall likewise survive "
        "for three (3) years post-termination."
    )

    pdf.sec(8, "Representations and Warranties")
    pdf.body(
        "Licensor represents and warrants that: (a) it has the full right and authority to license "
        "the Licensed Data to Licensee; (b) the Licensed Data was collected and compiled in "
        "compliance with all applicable privacy laws, including CCPA and GDPR; (c) the Licensed "
        "Data does not infringe the intellectual property rights of any third party; and "
        "(d) Licensor maintains appropriate privacy notices and has obtained the necessary consents "
        "for the data collection and use authorized herein."
    )

    pdf.add_page()
    pdf.sec(9, "Indemnification")
    pdf.sub(
        "9.1",
        "Licensor shall defend, indemnify, and hold harmless Licensee from any third-party "
        "claim alleging that the Licensed Data itself (not Licensee's use thereof) infringes "
        "any third-party intellectual property rights or violates any applicable privacy law "
        "as it relates to Licensor's collection of the data.",
    )
    pdf.sub(
        "9.2",
        "Licensee shall defend, indemnify, and hold harmless Licensor from any third-party "
        "claim arising from Licensee's: (a) use of the Licensed Data outside the Permitted Purposes; "
        "(b) violation of any Prohibited Use; or (c) failure to maintain required client data "
        "protection agreements under Section 4.2.",
    )

    pdf.sec(10, "Term and Renewal")
    pdf.body(
        "This Agreement shall commence on the Effective Date and continue for one (1) year "
        '(the "Initial Term"). Thereafter, this Agreement shall automatically renew for '
        "successive one-year terms unless either party provides at least sixty (60) days prior "
        "written notice of non-renewal. Upon renewal, the License Fee shall increase by five "
        "percent (5%) over the prior year's fee unless otherwise agreed in writing."
    )

    pdf.sec(11, "Governing Law and Disputes")
    pdf.body(
        "This Agreement shall be governed by the laws of the Commonwealth of Massachusetts, "
        "without regard to conflict of law principles. Any dispute shall first be subject to "
        "a thirty (30) day good-faith negotiation period. Unresolved disputes shall be submitted "
        "to binding arbitration under the American Arbitration Association Commercial Rules, "
        "with proceedings held in Boston, Massachusetts. Either party may seek injunctive or "
        "other equitable relief from any court of competent jurisdiction to prevent irreparable harm."
    )
    pdf.ln(4)
    pdf.sig(
        'FOR MERKLE INC. ("Licensee")', 'FOR APEX IDENTITY SOLUTIONS INC. ("Licensor")'
    )
    pdf.output(f"{OUT}/contract_005_data_usage_license.pdf")
    print("Written: contract_005_data_usage_license.pdf")


if __name__ == "__main__":
    c001()
    c002()
    c003()
    c004()
    c005()
    print("All 5 contracts generated.")
