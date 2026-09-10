"""
gen_part2.py
Generates contracts 006-010 as high-quality professional PDFs.
"""
import math
import os
import random
import tempfile
from fpdf import FPDF, XPos, YPos
from PIL import Image, ImageDraw, ImageFont

OUT_DIR = os.path.dirname(__file__)
FONT_DIR = "/opt/miniconda3/envs/sp_test0/lib/python3.13/site-packages/matplotlib/mpl-data/fonts/ttf"
FONT_REG = os.path.join(FONT_DIR, "DejaVuSans.ttf")
FONT_BOLD = os.path.join(FONT_DIR, "DejaVuSans-Bold.ttf")
FONT_OBL = os.path.join(FONT_DIR, "DejaVuSans-Oblique.ttf")


# ---------------------------------------------------------------------------
# ContractPDF  – text-based PDFs
# ---------------------------------------------------------------------------
class ContractPDF(FPDF):
    def __init__(self, title="CONTRACT") -> None:
        super().__init__(orientation="P", unit="mm", format="A4")
        self.contract_title = title
        self._total_pages = 0
        self.add_font("dv", fname=FONT_REG)
        self.add_font("dvb", fname=FONT_BOLD)
        self.add_font("dvi", fname=FONT_OBL)
        self.set_margins(left=22, top=25, right=22)
        self.set_auto_page_break(auto=True, margin=20)

    def header(self):
        w = self.w - self.l_margin - self.r_margin
        self.set_font("dvb", size=8)
        self.set_text_color(110, 110, 110)
        self.cell(w / 2, 6, "NEXACORE INC.  |  CONFIDENTIAL", align="L")
        self.cell(
            w / 2,
            6,
            self.contract_title,
            align="R",
            new_x=XPos.LMARGIN,
            new_y=YPos.NEXT,
        )
        self.set_draw_color(170, 170, 170)
        self.line(self.l_margin, self.get_y(), self.w - self.r_margin, self.get_y())
        self.ln(2)
        self.set_text_color(0, 0, 0)

    def footer(self):
        self.set_y(-15)
        self.set_font("dvi", size=8)
        self.set_text_color(130, 130, 130)
        self.cell(0, 8, f"Page {self.page_no()} of {{nb}}", align="R")
        self.set_text_color(0, 0, 0)

    def title_block(self, title, sub1="", sub2=""):
        w = self.w - self.l_margin - self.r_margin
        self.ln(2)
        self.set_font("dvb", size=15)
        self.multi_cell(w, 9, title, align="C")
        if sub1:
            self.set_font("dvi", size=10)
            self.multi_cell(w, 7, sub1, align="C")
        if sub2:
            self.set_font("dv", size=9)
            self.multi_cell(w, 6, sub2, align="C")
        self.ln(4)
        self.set_draw_color(60, 60, 60)
        self.line(self.l_margin, self.get_y(), self.w - self.r_margin, self.get_y())
        self.ln(4)

    def section(self, num, title):
        """Bold 11pt section header."""
        self.set_font("dvb", size=11)
        self.set_text_color(20, 20, 20)
        label = f"Section {num}. {title}" if num else title
        self.cell(0, 8, label, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_text_color(0, 0, 0)

    def body(self, text):
        """10pt multi_cell body text."""
        w = self.w - self.l_margin - self.r_margin
        self.set_font("dv", size=10)
        self.multi_cell(w, 6, text)
        self.ln(2)

    def sub(self, num, text):
        """8mm indent, 10pt subsection."""
        w = self.w - self.l_margin - self.r_margin - 8
        self.set_x(self.l_margin + 8)
        self.set_font("dv", size=10)
        self.multi_cell(w, 6, f"{num}  {text}")
        self.set_x(self.l_margin)
        self.ln(1)

    def bold_sub(self, num, label, text):
        """Bold subsection number+label, then normal text."""
        w = self.w - self.l_margin - self.r_margin
        self.set_font("dvb", size=10)
        self.set_x(self.l_margin)
        full = f"{num} {label}.  "
        bw = self.get_string_width(full)
        self.cell(bw, 6, full)
        self.set_font("dv", size=10)
        self.multi_cell(w - bw, 6, text)
        self.ln(1)

    def table(self, cols, rows, widths):
        """Bordered table, gray header row."""
        # header
        self.set_font("dvb", size=9)
        self.set_fill_color(70, 80, 95)
        self.set_text_color(255, 255, 255)
        for i, c in enumerate(cols):
            self.cell(widths[i], 8, c, border=1, fill=True, align="C")
        self.ln()
        self.set_text_color(0, 0, 0)
        # rows
        for ri, row in enumerate(rows):
            self.set_fill_color(245, 247, 250) if ri % 2 == 0 else self.set_fill_color(
                255, 255, 255
            )
            self.set_font("dv", size=9)
            # find tallest cell
            max_lines = 1
            for i, cell in enumerate(row):
                approx = math.ceil(self.get_string_width(str(cell)) / (widths[i] - 2))
                max_lines = max(max_lines, max(1, approx))
            rh = max(7, max_lines * 6)
            x0 = self.get_x()
            y0 = self.get_y()
            for i, cell in enumerate(row):
                self.set_xy(x0 + sum(widths[:i]), y0)
                self.multi_cell(
                    widths[i],
                    rh
                    / max(
                        1,
                        math.ceil(
                            self.get_string_width(str(cell)) / (widths[i] - 2)
                            if widths[i] > 2
                            else 1
                        ),
                    ),
                    str(cell),
                    border=0,
                    fill=(ri % 2 == 0),
                )
            # draw borders manually
            self.set_xy(x0, y0)
            for i in range(len(row)):
                self.cell(widths[i], rh, "", border=1)
            self.ln()
        self.ln(3)

    def table_simple(self, cols, rows, widths, row_height=7):
        """Simple single-line-per-cell table."""
        self.set_font("dvb", size=9)
        self.set_fill_color(70, 80, 95)
        self.set_text_color(255, 255, 255)
        for i, c in enumerate(cols):
            self.cell(widths[i], 8, c, border=1, fill=True, align="C")
        self.ln()
        self.set_text_color(0, 0, 0)
        for ri, row in enumerate(rows):
            self.set_fill_color(245, 247, 250) if ri % 2 == 0 else self.set_fill_color(
                255, 255, 255
            )
            self.set_font("dv", size=9)
            for i, cell in enumerate(row):
                self.cell(
                    widths[i],
                    row_height,
                    str(cell),
                    border=1,
                    fill=(ri % 2 == 0),
                    align="L",
                )
            self.ln()
        self.ln(3)

    def sig_block(
        self,
        party_a="NEXACORE INC.",
        party_b="COUNTERPARTY",
        name_a="",
        title_a="",
        name_b="",
        title_b="",
    ):
        self.ln(6)
        w = self.w - self.l_margin - self.r_margin
        col = w / 2 - 4
        self.set_font("dvb", size=10)
        self.cell(col, 7, f"FOR {party_a}:")
        self.cell(8, 7, "")
        self.cell(col, 7, f"FOR {party_b}:", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_font("dv", size=9)
        rows = [
            (
                "Signature:",
                "________________________________",
                "Signature:",
                "________________________________",
            ),
            (
                "Name:",
                name_a if name_a else "________________________________",
                "Name:",
                name_b if name_b else "________________________________",
            ),
            (
                "Title:",
                title_a if title_a else "________________________________",
                "Title:",
                title_b if title_b else "________________________________",
            ),
            (
                "Date:",
                "________________________________",
                "Date:",
                "________________________________",
            ),
        ]
        for la, va, lb, vb in rows:
            self.set_font("dvb", size=9)
            lw = self.get_string_width("Signature:  ")
            self.cell(lw, 7, la)
            self.set_font("dv", size=9)
            self.cell(col - lw, 7, va)
            self.cell(8, 7, "")
            self.set_font("dvb", size=9)
            self.cell(lw, 7, lb)
            self.set_font("dv", size=9)
            self.cell(col - lw, 7, vb, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.ln(2)


# ---------------------------------------------------------------------------
# CONTRACT 006 – Master Services Agreement  (8 pages, TEXT-BASED)
# ---------------------------------------------------------------------------
def contract_006():
    pdf = ContractPDF("MASTER SERVICES AGREEMENT")
    pdf.alias_nb_pages()

    # ── PAGE 1 ──────────────────────────────────────────────────────────────
    pdf.add_page()
    pdf.title_block(
        "MASTER SERVICES AGREEMENT",
        "Nexacore Inc. and VendorF Solutions Inc.",
        "Agreement No. MSA-2025-0019  |  Effective Date: January 15, 2025",
    )
    pdf.set_font("dvb", size=10)
    pdf.cell(0, 6, "PARTIES", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font("dv", size=10)
    pdf.body(
        'CLIENT:   Nexacore Inc., 100 Innovation Drive, Boston, MA 02101 ("Client")\n'
        'PROVIDER: VendorF Solutions Inc., 600 Innovation Circle, Denver, CO 80202 ("Service Provider")'
    )
    pdf.set_font("dvb", size=10)
    pdf.cell(0, 6, "RECITALS", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font("dv", size=10)
    pdf.body(
        "WHEREAS, Client is a leading data-driven marketing services company that requires "
        "identity resolution, data enrichment, and audience activation services to support its "
        "client engagements and internal analytics operations;"
    )
    pdf.body(
        "WHEREAS, Service Provider is in the business of providing identity resolution platforms, "
        "consumer data linkage services, and related analytics and API-based data services, and "
        "represents that it has the expertise, technology, and resources necessary to provide "
        "such services to Client on the terms set forth herein;"
    )
    pdf.body(
        "WHEREAS, Client and Service Provider desire to enter into this Master Services Agreement "
        "to establish the terms and conditions under which Service Provider will provide Services "
        "to Client from time to time pursuant to Statements of Work executed hereunder;"
    )
    pdf.body(
        "NOW, THEREFORE, in consideration of the mutual covenants and agreements hereinafter set forth, "
        "and for other good and valuable consideration, the receipt and sufficiency of which are hereby "
        "acknowledged, the parties agree as follows:"
    )

    pdf.section(1, "DEFINITIONS")
    pdf.body(
        "As used in this Agreement, the following capitalized terms shall have the meanings set forth below:"
    )
    defs = [
        (
            "1.1",
            '"Agreement"',
            "means this Master Services Agreement, together with all Statements of Work, exhibits, schedules, "
            "and amendments executed by the parties from time to time, each of which is incorporated herein by reference.",
        ),
        (
            "1.2",
            '"Client Data"',
            "means all data, information, and materials provided by Client to Service Provider in connection with "
            "the Services, including Client's customer lists, prospect files, campaign data, and any data derived "
            "therefrom that contains or is derived from Client's proprietary information.",
        ),
        (
            "1.3",
            '"Confidential Information"',
            "means any non-public information disclosed by one party to the other party in connection with this "
            "Agreement, whether disclosed orally, in writing, or by any other means, that is designated as "
            "confidential or that a reasonable person would understand to be confidential given the nature of "
            "the information and circumstances of disclosure.",
        ),
        (
            "1.4",
            '"Deliverables"',
            "means all work product, reports, data files, analyses, software, documentation, and other materials "
            "that Service Provider is obligated to deliver to Client under a Statement of Work or this Agreement.",
        ),
        (
            "1.5",
            '"Fees"',
            "means all amounts payable by Client to Service Provider under this Agreement, including fixed fees, "
            "usage-based fees, overage charges, and any other compensation as specified in the applicable "
            "Statement of Work.",
        ),
        (
            "1.6",
            '"Intellectual Property"',
            "means all patents, patent applications, copyrights, trademarks, trade secrets, know-how, and all "
            "other intellectual property rights, whether registered or unregistered, recognized under the laws "
            "of any jurisdiction.",
        ),
        (
            "1.7",
            '"Services"',
            "means the identity resolution, data enrichment, audience activation, analytics, and related "
            "professional services to be performed by Service Provider for Client as described in the applicable "
            "Statement of Work.",
        ),
        (
            "1.8",
            '"Statement of Work" or "SOW"',
            "means a written document executed by authorized representatives of both parties that describes "
            "specific Services, Deliverables, timelines, and Fees for a particular engagement, and that "
            "incorporates and is governed by the terms and conditions of this Agreement.",
        ),
    ]
    for num, term, defn in defs:
        w = pdf.w - pdf.l_margin - pdf.r_margin
        pdf.set_x(pdf.l_margin + 6)
        pdf.set_font("dvb", size=10)
        tw = pdf.get_string_width(f"{num}  {term}  ")
        pdf.cell(tw, 6, f"{num}  {term}  ")
        pdf.set_font("dv", size=10)
        pdf.multi_cell(w - 6 - tw, 6, defn)
        pdf.ln(1)

    # ── PAGE 2 ──────────────────────────────────────────────────────────────
    pdf.add_page()
    pdf.section(2, "SERVICES")
    pdf.bold_sub(
        "2.1",
        "Scope of Services",
        "Service Provider shall perform the Services as described in each Statement of Work executed by "
        "the parties. Service Provider shall provide all personnel, equipment, software, and other resources "
        "necessary to perform the Services in accordance with the standards set forth herein. Service Provider "
        "shall perform the Services in a professional and workmanlike manner, consistent with industry "
        "standards and best practices for the type of services being rendered.",
    )
    pdf.bold_sub(
        "2.2",
        "Statements of Work",
        "The parties shall execute a Statement of Work for each distinct engagement prior to commencement "
        "of the applicable Services. Each SOW shall include, at minimum: (a) a description of the Services "
        "to be performed; (b) a list of Deliverables and applicable acceptance criteria; (c) a delivery "
        "schedule and project timeline; (d) applicable Fees and payment terms; and (e) any special terms "
        "or conditions applicable to the specific engagement. In the event of any conflict between a "
        "Statement of Work and this Agreement, the terms of this Agreement shall control unless the SOW "
        "expressly states that it supersedes a specific provision of this Agreement.",
    )
    pdf.bold_sub(
        "2.3",
        "Change Orders",
        "Either party may request changes to the scope, timeline, or Fees for any SOW by submitting a "
        "written change order request to the other party. No change to an SOW shall be effective unless "
        "evidenced by a written change order signed by authorized representatives of both parties. Service "
        "Provider shall not be obligated to perform any additional services or incur additional costs "
        "beyond those specified in the applicable SOW without an executed change order.",
    )
    pdf.bold_sub(
        "2.4",
        "Subcontractors",
        "Service Provider shall not subcontract any portion of the Services to a third party without "
        "Client's prior written approval, which shall not be unreasonably withheld or delayed. Any approved "
        "subcontractor shall be bound by confidentiality and data security obligations at least as "
        "protective as those set forth in this Agreement. Service Provider shall remain primarily "
        "responsible for all acts and omissions of its subcontractors.",
    )

    pdf.section(3, "FEES AND PAYMENT")
    pdf.bold_sub(
        "3.1",
        "Fee Structure",
        "Client shall pay Service Provider the Fees set forth in each applicable Statement of Work. "
        "Fees may be structured as fixed monthly or annual fees, usage-based fees, or a combination "
        "thereof, as specified in the applicable SOW. All Fees are denominated in United States dollars "
        "and are exclusive of applicable taxes.",
    )
    pdf.bold_sub(
        "3.2",
        "Invoicing and Payment — Net 30",
        "Service Provider shall issue invoices to Client on the schedule specified in the applicable "
        "SOW, or if not specified, monthly in arrears. All invoices are due and payable within thirty "
        '(30) days of receipt ("Net 30"). Invoices shall be submitted electronically to Client\'s '
        "accounts payable department at the address specified in the applicable SOW. Overdue amounts "
        "shall accrue interest at the rate of one and one-half percent (1.5%) per month, or the "
        "maximum rate permitted by applicable law, whichever is less.",
    )
    pdf.bold_sub(
        "3.3",
        "Disputed Invoices",
        "Client shall notify Service Provider in writing of any disputed invoice amount within fifteen "
        "(15) days of receipt. The parties shall use good-faith efforts to resolve any billing dispute "
        "within thirty (30) days of such notice. Client shall pay all undisputed invoice amounts on "
        "time. Disputed amounts shall be held in good faith pending resolution and shall not be subject "
        "to late interest during the resolution period, provided Client has given timely written notice "
        "of the dispute.",
    )
    pdf.bold_sub(
        "3.4",
        "Taxes",
        "Client shall be responsible for all applicable sales, use, value-added, and similar taxes "
        "imposed by any governmental authority on the Fees paid under this Agreement, excluding taxes "
        "imposed on Service Provider's net income. Service Provider shall separately state any such "
        "taxes on its invoices. Each party shall cooperate to minimize applicable taxes and shall "
        "provide the other party with any applicable tax exemption certificates or other documentation "
        "reasonably requested.",
    )

    # ── PAGE 3 ──────────────────────────────────────────────────────────────
    pdf.add_page()
    pdf.section(4, "INTELLECTUAL PROPERTY")
    pdf.bold_sub(
        "4.1",
        "Client Data Ownership",
        "As between the parties, Client shall own all right, title, and interest in and to the Client "
        "Data. Service Provider shall not use Client Data for any purpose other than performing the "
        "Services under this Agreement. Service Provider shall not disclose Client Data to any third "
        "party except as necessary to perform the Services and as authorized in writing by Client.",
    )
    pdf.bold_sub(
        "4.2",
        "Service Provider Background IP",
        "Service Provider shall retain all right, title, and interest in and to its pre-existing "
        "intellectual property, including its identity graph, resolution algorithms, data processing "
        "platforms, methodologies, software, and any improvements or derivatives thereof developed "
        'independently of Client ("Background IP"). Nothing in this Agreement shall be construed '
        "to transfer or assign any Background IP to Client.",
    )
    pdf.bold_sub(
        "4.3",
        "Work Product Ownership",
        "All Deliverables created specifically for Client under a Statement of Work, including custom "
        "reports, derived data sets, and analyses, shall be owned by Client upon full payment of all "
        "applicable Fees, to the extent such Deliverables do not incorporate Service Provider's "
        "Background IP. To the extent Deliverables incorporate Background IP, Service Provider "
        "grants Client a limited, non-exclusive, perpetual license to use such Background IP solely "
        "as incorporated in the Deliverables and for Client's internal business purposes.",
    )
    pdf.bold_sub(
        "4.4",
        "License Grants",
        "Service Provider grants Client a non-exclusive, non-transferable, limited license to access "
        "and use the Services and any standard platform outputs during the term of the applicable "
        "SOW, solely for Client's internal business purposes. Client grants Service Provider a "
        "limited license to use Client Data solely to the extent necessary to perform the Services.",
    )

    pdf.section(5, "DATA PRIVACY AND SECURITY")
    pdf.bold_sub(
        "5.1",
        "Data Handling Obligations",
        "Service Provider shall process Client Data only in accordance with Client's written "
        "instructions and as necessary to perform the Services. Service Provider shall not process "
        "Client Data for any purpose not authorized by this Agreement or a Statement of Work. "
        "Service Provider shall implement and maintain appropriate technical and organizational "
        "measures to protect Client Data against unauthorized access, disclosure, alteration, "
        "or destruction.",
    )
    pdf.bold_sub(
        "5.2",
        "Security Standards — SOC 2 Type II",
        "Service Provider shall maintain a security program that meets or exceeds the requirements "
        "of SOC 2 Type II certification throughout the term of this Agreement. Service Provider "
        "shall provide Client with its current SOC 2 Type II audit report upon request and shall "
        "notify Client promptly if its SOC 2 certification lapses or is placed under qualification. "
        "Service Provider shall also comply with applicable data security frameworks as agreed in "
        "writing by the parties.",
    )
    pdf.bold_sub(
        "5.3",
        "Breach Notification — 48 Hours",
        "Service Provider shall notify Client in writing within forty-eight (48) hours of "
        "discovering any actual or reasonably suspected unauthorized access to, or acquisition, "
        'disclosure, or use of, Client Data (a "Security Incident"). Such notice shall include, '
        "to the extent then known: (a) a description of the nature of the Security Incident; "
        "(b) the categories and approximate number of records affected; (c) the likely consequences "
        "of the Security Incident; and (d) the measures Service Provider has taken or proposes to "
        "take to address the Security Incident.",
    )
    pdf.bold_sub(
        "5.4",
        "Data Retention and Deletion",
        "Service Provider shall retain Client Data only for as long as necessary to perform the "
        "Services or as required by applicable law. Upon expiration or termination of this Agreement "
        "or any applicable SOW, or upon Client's written request, Service Provider shall promptly "
        "return or securely delete all Client Data, and shall provide written certification of "
        "deletion within thirty (30) days of such request or termination.",
    )

    # ── PAGE 4 ──────────────────────────────────────────────────────────────
    pdf.add_page()
    pdf.section(6, "CONFIDENTIALITY")
    pdf.bold_sub(
        "6.1",
        "Confidentiality Obligations",
        'Each party (as "Receiving Party") agrees to: (a) hold the other party\'s ("Disclosing '
        "Party's\") Confidential Information in strict confidence; (b) use Confidential Information "
        "solely for the purposes of this Agreement; (c) not disclose Confidential Information to "
        "any third party without the Disclosing Party's prior written consent; and (d) limit "
        "disclosure to employees, contractors, and advisors who have a legitimate need to know "
        "and are bound by confidentiality obligations no less protective than those set forth herein.",
    )
    pdf.bold_sub(
        "6.2",
        "Exclusions",
        "Confidentiality obligations under this Agreement do not apply to information that: (a) is "
        "or becomes publicly available through no fault of the Receiving Party; (b) was rightfully "
        "known to the Receiving Party without restriction before disclosure; (c) is independently "
        "developed by the Receiving Party without use of the Disclosing Party's Confidential "
        "Information; or (d) is required to be disclosed by law, regulation, or court order, "
        "provided that the Receiving Party gives the Disclosing Party prompt prior written notice "
        "to the extent legally permissible and cooperates with the Disclosing Party's efforts to "
        "seek a protective order or other appropriate relief.",
    )
    pdf.bold_sub(
        "6.3",
        "Survival",
        "Confidentiality obligations under this Section 6 shall survive the expiration or "
        "termination of this Agreement for a period of two (2) years following such expiration "
        "or termination, except with respect to trade secrets, which shall be protected for so "
        "long as such information constitutes a trade secret under applicable law.",
    )
    pdf.bold_sub(
        "6.4",
        "Injunctive Relief",
        "Each party acknowledges that a breach of the confidentiality obligations set forth "
        "in this Section 6 would cause irreparable harm for which monetary damages would be "
        "an inadequate remedy. Accordingly, each party shall be entitled to seek injunctive "
        "or other equitable relief to prevent or remedy such breach, without the requirement "
        "of posting a bond or proving actual damages.",
    )

    pdf.section(7, "REPRESENTATIONS AND WARRANTIES")
    pdf.bold_sub(
        "7.1",
        "Authority",
        "Each party represents and warrants that: (a) it is duly organized, validly existing, "
        "and in good standing under the laws of its jurisdiction of formation; (b) it has full "
        "power and authority to enter into this Agreement and to perform its obligations hereunder; "
        "and (c) this Agreement has been duly authorized, executed, and delivered by such party "
        "and constitutes the legal, valid, and binding obligation of such party, enforceable "
        "against it in accordance with its terms.",
    )
    pdf.bold_sub(
        "7.2",
        "No Conflicts",
        "Each party represents and warrants that the execution, delivery, and performance of this "
        "Agreement does not and will not: (a) conflict with or violate any applicable law, "
        "regulation, or court order; (b) conflict with or violate any provision of such party's "
        "organizational documents; or (c) conflict with or result in a breach of any agreement, "
        "contract, or obligation to which such party is a party or by which it is bound.",
    )
    pdf.bold_sub(
        "7.3",
        "Compliance with Laws",
        "Service Provider represents and warrants that it will perform the Services in compliance "
        "with all applicable federal, state, and local laws and regulations, including applicable "
        "privacy and data protection laws (including CCPA, CAN-SPAM, and other relevant statutes), "
        "and that its data collection and processing practices comply with all applicable legal "
        "requirements and consumer consent obligations.",
    )
    pdf.bold_sub(
        "7.4",
        "Data Accuracy",
        "Service Provider represents and warrants that: (a) all data delivered as part of the "
        "Services meets the accuracy and quality standards specified in the applicable SOW; "
        "(b) Service Provider has the right to license and deliver such data to Client; "
        "(c) the data does not infringe any third-party intellectual property rights; and "
        "(d) Service Provider has obtained all necessary consents and authorizations required "
        "by applicable law for the collection, processing, and delivery of such data.",
    )

    # ── PAGE 5 ──────────────────────────────────────────────────────────────
    pdf.add_page()
    pdf.section(8, "INDEMNIFICATION")
    pdf.bold_sub(
        "8.1",
        "Client Indemnification",
        "Client shall indemnify, defend, and hold harmless Service Provider and its officers, "
        "directors, employees, and agents from and against any and all claims, damages, losses, "
        "liabilities, costs, and expenses (including reasonable attorneys' fees) arising out of "
        "or relating to: (a) Client's use of the Services in violation of this Agreement or "
        "applicable law; (b) Client's use of Client Data or the Deliverables in a manner not "
        "authorized by this Agreement; or (c) Client's breach of any representation, warranty, "
        "or obligation under this Agreement.",
    )
    pdf.bold_sub(
        "8.2",
        "Service Provider Indemnification — IP Infringement",
        "Service Provider shall indemnify, defend, and hold harmless Client and its officers, "
        "directors, employees, and agents from and against any and all claims, damages, losses, "
        "liabilities, costs, and expenses (including reasonable attorneys' fees) arising out of "
        "or relating to: (a) any claim that the Services or Deliverables infringe or misappropriate "
        "any third-party intellectual property right; (b) Service Provider's breach of its data "
        "privacy, security, or confidentiality obligations under this Agreement; or (c) Service "
        "Provider's breach of any representation, warranty, or obligation under this Agreement.",
    )
    pdf.bold_sub(
        "8.3",
        "Mutual Defense Obligations",
        "Each party's indemnification obligations are conditioned on the indemnified party: "
        "(a) promptly notifying the indemnifying party in writing of any claim for which "
        "indemnification is sought; (b) granting the indemnifying party sole control over "
        "the defense and settlement of such claim, provided that the indemnifying party shall "
        "not settle any claim in a manner that imposes liability or obligation on the "
        "indemnified party without the indemnified party's prior written consent; and "
        "(c) providing the indemnifying party with reasonable assistance, at the indemnifying "
        "party's expense.",
    )
    pdf.bold_sub(
        "8.4",
        "Indemnification Procedure",
        "The indemnifying party shall have the right to select counsel for the defense of any "
        "indemnified claim, subject to the indemnified party's approval, which shall not be "
        "unreasonably withheld. The indemnified party shall have the right to participate in "
        "the defense of any claim at its own expense. Settlement of any claim that requires "
        "the indemnified party to admit fault, pay money, or accept ongoing obligations shall "
        "require the indemnified party's written consent.",
    )

    pdf.section(9, "LIMITATION OF LIABILITY")
    pdf.bold_sub(
        "9.1",
        "Exclusion of Consequential Damages",
        "IN NO EVENT SHALL EITHER PARTY BE LIABLE TO THE OTHER PARTY FOR ANY INDIRECT, INCIDENTAL, "
        "SPECIAL, PUNITIVE, OR CONSEQUENTIAL DAMAGES, INCLUDING LOSS OF PROFITS, LOSS OF REVENUE, "
        "LOSS OF DATA, OR BUSINESS INTERRUPTION, ARISING OUT OF OR RELATED TO THIS AGREEMENT, "
        "REGARDLESS OF THE FORM OF ACTION AND WHETHER SUCH PARTY HAS BEEN ADVISED OF THE "
        "POSSIBILITY OF SUCH DAMAGES.",
    )
    pdf.bold_sub(
        "9.2",
        "Aggregate Liability Cap",
        "EACH PARTY'S AGGREGATE LIABILITY TO THE OTHER PARTY FOR ALL CLAIMS ARISING OUT OF OR "
        "RELATED TO THIS AGREEMENT SHALL NOT EXCEED THE TOTAL FEES ACTUALLY PAID OR PAYABLE BY "
        "CLIENT TO SERVICE PROVIDER DURING THE TWELVE (12) MONTH PERIOD IMMEDIATELY PRECEDING "
        "THE DATE THE CLAIM AROSE.",
    )
    pdf.bold_sub(
        "9.3",
        "Exceptions",
        "The limitations set forth in Sections 9.1 and 9.2 shall not apply to: (a) a party's "
        "liability for fraud or willful misconduct; (b) Service Provider's indemnification "
        "obligations for intellectual property infringement under Section 8.2; or (c) a party's "
        "liability for breach of its confidentiality obligations under Section 6.",
    )

    # ── PAGE 6 ──────────────────────────────────────────────────────────────
    pdf.add_page()
    pdf.section(10, "INSURANCE")
    pdf.bold_sub(
        "10.1",
        "Commercial General Liability",
        "Service Provider shall maintain, at its own expense, commercial general liability "
        "insurance with limits of not less than Two Million Dollars ($2,000,000) per occurrence "
        "and Four Million Dollars ($4,000,000) in the aggregate, covering bodily injury, property "
        "damage, personal injury, and advertising injury.",
    )
    pdf.bold_sub(
        "10.2",
        "Professional Liability / Errors & Omissions",
        "Service Provider shall maintain professional liability (errors and omissions) insurance "
        "with limits of not less than Five Million Dollars ($5,000,000) per claim and in the "
        "aggregate, covering claims arising out of negligent acts, errors, or omissions in the "
        "performance of the Services.",
    )
    pdf.bold_sub(
        "10.3",
        "Cyber Liability",
        "Service Provider shall maintain cyber liability and data breach insurance with limits "
        "of not less than Ten Million Dollars ($10,000,000) per occurrence and in the aggregate, "
        "covering claims arising out of data breaches, security incidents, and privacy violations.",
    )
    pdf.bold_sub(
        "10.4",
        "Certificates of Insurance",
        "Service Provider shall provide Client with certificates of insurance evidencing the "
        "coverage required under this Section 10 upon request and upon renewal of any policy. "
        "Such certificates shall name Client as an additional insured with respect to the "
        "commercial general liability policy. Service Provider shall provide Client with thirty "
        "(30) days prior written notice of any material change or cancellation of coverage.",
    )

    pdf.section(11, "TERM AND TERMINATION")
    pdf.bold_sub(
        "11.1",
        "Initial Term",
        "This Agreement shall commence on the Effective Date and continue for an initial term "
        'of two (2) years (the "Initial Term"), unless earlier terminated as provided herein.',
    )
    pdf.bold_sub(
        "11.2",
        "Renewal",
        "Following the Initial Term, this Agreement shall automatically renew for successive "
        "one (1) year terms unless either party provides the other with written notice of "
        "non-renewal at least sixty (60) days prior to the end of the then-current term.",
    )
    pdf.bold_sub(
        "11.3",
        "Termination for Convenience",
        "Either party may terminate this Agreement for any reason or no reason upon ninety "
        "(90) days prior written notice to the other party. Such termination shall not relieve "
        "Client of its obligation to pay Fees for Services rendered prior to the effective date "
        "of termination.",
    )
    pdf.bold_sub(
        "11.4",
        "Termination for Cause",
        "Either party may terminate this Agreement immediately upon written notice if the other "
        "party materially breaches this Agreement and fails to cure such breach within thirty "
        "(30) days after receiving written notice describing the breach in reasonable detail; "
        "provided, however, that if the breach is not reasonably capable of cure within thirty "
        "(30) days, the breaching party shall have such additional time as is reasonably necessary "
        "to effect a cure, so long as it commences cure within such thirty (30) day period and "
        "diligently pursues cure to completion.",
    )
    pdf.bold_sub(
        "11.5",
        "Effects of Termination",
        "Upon expiration or termination: (a) all SOWs then in effect shall also terminate, "
        "unless the parties agree otherwise in writing; (b) each party shall promptly return "
        "or destroy the other party's Confidential Information; (c) Service Provider shall "
        "return or delete all Client Data as provided in Section 5.4; (d) Client shall pay "
        "all outstanding Fees for Services rendered through the termination date; and "
        "(e) Sections 1, 4, 6, 7, 8, 9, 11.5, and 12 shall survive termination.",
    )

    # ── PAGE 7 ──────────────────────────────────────────────────────────────
    pdf.add_page()
    pdf.section(12, "GENERAL PROVISIONS")
    pdf.bold_sub(
        "12.1",
        "Force Majeure",
        "Neither party shall be liable for any delay or failure to perform its obligations "
        "under this Agreement to the extent such delay or failure is caused by circumstances "
        "beyond such party's reasonable control, including acts of God, natural disasters, "
        "war, terrorism, pandemic, governmental actions, or internet or telecommunications "
        "failures not caused by such party's negligence. The affected party shall promptly "
        "notify the other party and use commercially reasonable efforts to resume performance.",
    )
    pdf.bold_sub(
        "12.2",
        "Entire Agreement",
        "This Agreement, together with all Statements of Work and exhibits incorporated herein, "
        "constitutes the entire agreement between the parties with respect to the subject matter "
        "hereof and supersedes all prior and contemporaneous negotiations, representations, "
        "warranties, and agreements, whether oral or written, relating to such subject matter.",
    )
    pdf.bold_sub(
        "12.3",
        "Amendments",
        "This Agreement may not be amended, modified, or supplemented except by a written "
        "instrument duly executed by authorized representatives of both parties.",
    )
    pdf.bold_sub(
        "12.4",
        "Waiver",
        "No waiver of any provision of this Agreement shall be effective unless in writing "
        "and signed by the party against whom such waiver is asserted. No waiver shall be "
        "deemed a waiver of any subsequent breach or default of the same or any other provision.",
    )
    pdf.bold_sub(
        "12.5",
        "Notices",
        "All notices under this Agreement shall be in writing and shall be deemed delivered "
        "when: (a) delivered personally; (b) sent by overnight courier with confirmation; "
        "(c) sent by certified mail, return receipt requested; or (d) sent by email with "
        "confirmed receipt. Notices to Client shall be addressed to its principal office "
        "at the address set forth on page 1, Attention: Legal Department. Notices to Service "
        "Provider shall be addressed to its principal office at the address set forth on page 1, "
        "Attention: General Counsel.",
    )
    pdf.bold_sub(
        "12.6",
        "Assignment",
        "Neither party may assign this Agreement or any rights or obligations hereunder "
        "without the prior written consent of the other party, which shall not be unreasonably "
        "withheld or delayed; except that either party may assign this Agreement without "
        "consent in connection with a merger, acquisition, or sale of all or substantially "
        "all of its assets, provided that the assignee assumes all obligations hereunder.",
    )
    pdf.bold_sub(
        "12.7",
        "Counterparts and Electronic Signatures",
        "This Agreement may be executed in one or more counterparts, each of which shall "
        "be deemed an original and all of which together shall constitute one and the same "
        "instrument. Electronic signatures shall be deemed valid and binding to the same "
        "extent as original signatures.",
    )
    pdf.bold_sub(
        "12.8",
        "Governing Law",
        "This Agreement shall be governed by and construed in accordance with the laws of "
        "the State of Maryland, without regard to its conflict of law principles.",
    )
    pdf.bold_sub(
        "12.9",
        "Dispute Resolution",
        "The parties shall attempt to resolve any dispute arising out of or relating to "
        "this Agreement through good-faith negotiation. If the dispute is not resolved "
        "within thirty (30) days of written notice, the parties shall submit to non-binding "
        "mediation before a mutually agreed mediator in Baltimore, Maryland. If mediation "
        "fails to resolve the dispute within sixty (60) days, the dispute shall be submitted "
        "to binding arbitration under the Commercial Arbitration Rules of the American "
        "Arbitration Association, with proceedings to be held in Baltimore, Maryland.",
    )
    pdf.bold_sub(
        "12.10",
        "Severability",
        "If any provision of this Agreement is held to be invalid, illegal, or unenforceable "
        "under applicable law, such provision shall be modified to the minimum extent necessary "
        "to make it valid and enforceable, and the remaining provisions shall continue in "
        "full force and effect.",
    )

    # ── PAGE 8 – EXHIBIT A ──────────────────────────────────────────────────
    pdf.add_page()
    pdf.set_font("dvb", size=12)
    pdf.cell(
        0,
        8,
        "EXHIBIT A — INITIAL STATEMENT OF WORK",
        new_x=XPos.LMARGIN,
        new_y=YPos.NEXT,
    )
    pdf.set_font("dv", size=10)
    pdf.body(
        'This Statement of Work ("SOW-001") is entered into as of January 15, 2025 and is '
        "incorporated into and governed by the Master Services Agreement between Nexacore Inc. "
        '("Client") and VendorF Solutions Inc. ("Service Provider") dated January 15, 2025. '
        "Capitalized terms not defined herein shall have the meanings ascribed in the Agreement."
    )
    pdf.body(
        "SERVICE DESCRIPTION: Service Provider shall deliver the identity resolution and audience "
        "activation services set forth below on a monthly subscription basis. All Deliverables "
        "shall meet the quality, format, and timeliness standards specified herein."
    )
    pdf.set_font("dvb", size=10)
    pdf.cell(
        0, 6, "SERVICES, DELIVERABLES, AND FEES:", new_x=XPos.LMARGIN, new_y=YPos.NEXT
    )
    pdf.ln(1)

    cols6 = ["Service", "Description", "Monthly Deliverable", "Fee"]
    widths6 = [42, 58, 45, 21]
    rows6 = [
        [
            "Consumer Identity Spine",
            "Monthly identity resolution file, 500M+ records, full consumer universe",
            "Full file delivery by 3rd business day",
            "$45,000/mo",
        ],
        [
            "Cross-Device Graph",
            "Bi-weekly device linkage file connecting mobile, desktop, CTV identifiers",
            "Delivery within 48 hrs of period close",
            "$22,500/mo",
        ],
        [
            "Audience Activation API",
            "Real-time REST API for audience lookups and identity matching, 99.9% uptime SLA",
            "99.9% uptime SLA; monthly usage report",
            "$18,000/mo",
        ],
        [
            "Quarterly Analytics Report",
            "Campaign effectiveness analysis and data quality assessment per quarter",
            "Within 15 days of quarter end",
            "$5,500/qtr",
        ],
    ]
    pdf.table_simple(cols6, rows6, widths6, row_height=14)

    pdf.body(
        "TOTAL ESTIMATED ANNUAL FEES: $1,026,000 (based on 12 months of monthly services plus "
        "4 quarterly analytics reports). Fees are subject to adjustment based on actual usage "
        "and any change orders executed during the term of this SOW."
    )
    pdf.body(
        "PAYMENT TERMS: Service Provider shall invoice Client monthly in arrears for all "
        "monthly services. Quarterly Analytics Reports shall be invoiced upon delivery. "
        "All invoices are due Net 30 days from receipt."
    )
    pdf.body(
        "SOW TERM: This SOW shall commence on January 15, 2025 and continue for twelve (12) "
        "months, unless earlier terminated or extended by written amendment."
    )

    pdf.sig_block(
        party_a="NEXACORE INC.",
        party_b="VENDORF SOLUTIONS INC.",
        name_a="Alex Morgan",
        title_a="EVP, Data & Analytics",
        name_b="David Chen",
        title_b="Chief Revenue Officer",
    )

    pdf.output(os.path.join(OUT_DIR, "contract_006_master_services_agreement.pdf"))
    print("Created contract_006_master_services_agreement.pdf")


# ---------------------------------------------------------------------------
# IMAGE-BASED helpers
# ---------------------------------------------------------------------------
def _squiggle(draw, x0, y0, length=260, color=(30, 30, 30)):
    """Draw a handwriting-style squiggly signature line."""
    points = []
    steps = 60
    for i in range(steps + 1):
        px = x0 + int(length * i / steps)
        py = y0 + random.randint(-6, 6)
        points.append((px, py))
    for i in range(len(points) - 1):
        draw.line([points[i], points[i + 1]], fill=color, width=3)


def _make_aged_page(bg_color=(240, 235, 224)):
    """Return (img, draw) for a 2480x3508 aged-paper page."""
    W, H = 2480, 3508
    img = Image.new("RGB", (W, H), bg_color)
    draw = ImageDraw.Draw(img)
    px = img.load()
    for _ in range(2000):
        x = random.randint(0, W - 1)
        y = random.randint(0, H - 1)
        v = random.randint(-5, 5)
        r, g, b = px[x, y]
        px[x, y] = (
            max(0, min(255, r + v)),
            max(0, min(255, g + v)),
            max(0, min(255, b + v)),
        )
    return img, draw


def _fonts(size_reg=44, size_bold=52, size_sm=36):
    try:
        fr = ImageFont.truetype(FONT_REG, size_reg)
        fb = ImageFont.truetype(FONT_BOLD, size_bold)
        fs = ImageFont.truetype(FONT_REG, size_sm)
    except Exception:
        fr = fb = fs = ImageFont.load_default()
    return fr, fb, fs


def _save_page(img):
    tmp = tempfile.NamedTemporaryFile(suffix=".jpg", delete=False)
    img.save(tmp.name, "JPEG", quality=88)
    tmp.close()
    return tmp.name


def _text_block(
    draw,
    lines,
    font_reg,
    font_bold,
    x=210,
    start_y=180,
    line_gap=62,
    bold_gap=72,
    fg=(28, 28, 28),
):
    """
    lines: list of (bold:bool, text:str)
    Returns final y position.
    """
    y = start_y
    for bold, text in lines:
        font = font_bold if bold else font_reg
        draw.text((x, y), text, font=font, fill=fg)
        y += bold_gap if bold else line_gap
    return y


# ---------------------------------------------------------------------------
# CONTRACT 007 – Legacy Renewal Scanned  (3 pages, IMAGE-BASED)
# ---------------------------------------------------------------------------
def contract_007():
    pdf = FPDF(orientation="P", unit="mm", format="A4")
    pdf.set_auto_page_break(False)

    fr, fb, fs = _fonts(44, 52, 36)
    fg = (28, 28, 28)
    BG = (240, 235, 224)  # #F0EBE0

    # ── PAGE 1 ──────────────────────────────────────────────────────────────
    img, draw = _make_aged_page(BG)
    # Header
    draw.text(
        (1240, 160), "RENEWAL OF DATA SUPPLY AGREEMENT", font=fb, fill=fg, anchor="mm"
    )
    draw.line([(180, 240), (2300, 240)], fill=(160, 150, 140), width=4)
    y = 290
    for text in [
        "Agreement No.: LDC-NXC-2024-R01",
        "Renewal Effective: January 1, 2024",
        "Renewal Term: January 1, 2024 through December 31, 2025",
    ]:
        draw.text((210, y), text, font=fr, fill=fg)
        y += 62
    y += 20
    draw.text((210, y), "Section 1.  PARTIES", font=fb, fill=fg)
    y += 72
    draw.text((210, y), 'NEXACORE CORPORATION ("NEXACORE")', font=fr, fill=fg)
    y += 62
    draw.text((210, y), "100 Innovation Drive, Boston, MA 02101", font=fs, fill=fg)
    y += 54
    draw.text((210, y), 'VENDORG DATACORP INC. ("VENDOR")', font=fr, fill=fg)
    y += 62
    draw.text(
        (210, y),
        "450 Technology Parkway, Suite 200, Atlanta, GA 30092",
        font=fs,
        fill=fg,
    )
    y += 80

    draw.text((210, y), "Section 2.  RENEWAL TERMS", font=fb, fill=fg)
    y += 72
    for line in [
        "This Agreement renews the Data Supply Agreement originally executed",
        'January 1, 2021 (the "Original Agreement") for an additional two (2) year',
        "period. All terms and conditions of the Original Agreement, including",
        "Exhibits A through D, remain in full force and effect except as expressly",
        "modified herein. In the event of conflict, this Renewal Agreement controls.",
        "",
        "The renewal is conditioned upon Vendor's continued compliance with all",
        "data security and privacy requirements, including annual SOC 2 Type II",
        "certification. Vendor shall deliver updated certification within 30 days",
        "of each annual audit completion.",
    ]:
        draw.text((210, y), line, font=fr, fill=fg)
        y += 56
    y += 20

    draw.text((210, y), "Section 3.  PRICING", font=fb, fill=fg)
    y += 72
    draw.text(
        (210, y),
        "Annual License Fee:  $195,000.00 per year (5% increase from prior term)",
        font=fr,
        fill=fg,
    )
    y += 62
    draw.text(
        (210, y),
        "Payment Schedule:    Quarterly, Net 30 days from invoice",
        font=fr,
        fill=fg,
    )
    y += 62
    draw.text((210, y), "Quarterly Amount:    $48,750.00 per quarter", font=fr, fill=fg)
    y += 62
    draw.text(
        (210, y),
        "Price Adjustment:    CPI-based annually thereafter, capped at 4%",
        font=fr,
        fill=fg,
    )
    y += 62

    tmp1 = _save_page(img)

    # ── PAGE 2 ──────────────────────────────────────────────────────────────
    img, draw = _make_aged_page(BG)
    draw.line([(180, 200), (2300, 200)], fill=(160, 150, 140), width=3)
    y = 240
    draw.text((210, y), "Section 4.  DATA SPECIFICATIONS", font=fb, fill=fg)
    y += 72
    specs = [
        (
            "Data Types:",
            "U.S. Consumer Identity File — name, address, email hash, phone hash,",
        ),
        ("", "date-of-birth month/year, household linkage identifier, deceased flag"),
        ("Record Universe:", "Approximately 240 million U.S. adult consumers"),
        ("Delivery Format:", "Fixed-width flat file, UTF-8 encoding, PGP encrypted"),
        ("Delivery Method:", "SFTP to Nexacore-designated endpoint"),
        ("Frequency:", "Full file monthly by the 5th business day; weekly delta files"),
        ("Delta Delivery:", "Every Monday by 6:00 AM Eastern Time"),
        ("Retention:", "Nexacore may retain data for 24 months from delivery date"),
    ]
    for label, value in specs:
        if label:
            draw.text((210, y), label, font=fb, fill=fg)
            draw.text((680, y), value, font=fr, fill=fg)
        else:
            draw.text((680, y), value, font=fr, fill=fg)
        y += 58
    y += 30

    draw.text((210, y), "Section 5.  GENERAL CONDITIONS", font=fb, fill=fg)
    y += 72
    for line in [
        "CONFIDENTIALITY: Both parties shall hold the other's Confidential Information",
        "in strict confidence. This obligation survives termination for three (3) years.",
        "",
        "TERMINATION: Either party may terminate this Agreement upon sixty (60) days",
        "prior written notice. Upon termination, Nexacore shall certify destruction of all",
        "Vendor data within 30 days and Vendor shall issue a pro-rata refund for any",
        "prepaid fees covering the post-termination period.",
        "",
        "GOVERNING LAW: This Agreement shall be governed by and construed under the",
        "laws of the State of Maryland, without regard to conflict of law principles.",
        "Any dispute not resolved by good-faith negotiation shall be submitted to",
        "binding arbitration in Baltimore, Maryland under AAA Commercial Rules.",
    ]:
        draw.text((210, y), line, font=fr, fill=fg)
        y += 56
    y += 30

    draw.text((210, y), "Section 6.  REPRESENTATIONS", font=fb, fill=fg)
    y += 72
    for line in [
        "Each party represents and warrants that: (i) it is duly organized and in good",
        "standing; (ii) it has full authority to execute this Renewal Agreement; (iii) this",
        "Renewal does not conflict with any other obligation of such party; and (iv) the",
        "individual executing this Agreement on behalf of such party is duly authorized.",
        "",
        "VENDOR further represents that all data provided complies with applicable federal",
        "and state privacy laws, including the CCPA, FCRA (where applicable), and CAN-SPAM,",
        "and that all consumer consent and opt-out records are current as of the renewal date.",
    ]:
        draw.text((210, y), line, font=fr, fill=fg)
        y += 56

    tmp2 = _save_page(img)

    # ── PAGE 3 ──────────────────────────────────────────────────────────────
    img, draw = _make_aged_page(BG)
    draw.line([(180, 200), (2300, 200)], fill=(160, 150, 140), width=3)
    y = 240
    draw.text((210, y), "Section 7.  ENTIRE AGREEMENT", font=fb, fill=fg)
    y += 72
    for line in [
        "This Renewal Agreement, together with the Original Agreement and all exhibits",
        "thereto, constitutes the entire agreement between the parties with respect to the",
        "subject matter hereof and supersedes all prior renewal discussions, letters of",
        "intent, and negotiations. No modification of this Agreement shall be binding",
        "unless made in writing and signed by authorized representatives of both parties.",
    ]:
        draw.text((210, y), line, font=fr, fill=fg)
        y += 56
    y += 50

    # Signature block
    draw.text((210, y), "FOR NEXACORE CORPORATION:", font=fb, fill=fg)
    draw.text((1300, y), "FOR VENDORG DATACORP INC.:", font=fb, fill=fg)
    y += 80

    draw.text((210, y), "Signature:", font=fr, fill=fg)
    draw.text((1300, y), "Signature:", font=fr, fill=fg)
    y += 50
    _squiggle(draw, 390, y, 340)
    _squiggle(draw, 1480, y, 340)
    y += 70

    draw.text((210, y), "Name:   Alex Morgan", font=fr, fill=fg)
    draw.text((1300, y), "Name:   Casey Thompson", font=fr, fill=fg)
    y += 62
    draw.text((210, y), "Title:  EVP, Data & Analytics", font=fr, fill=fg)
    draw.text((1300, y), "Title:  President", font=fr, fill=fg)
    y += 62
    draw.text((210, y), "Date:   December 12, 2023", font=fr, fill=fg)
    draw.text((1300, y), "Date:   December 14, 2023", font=fr, fill=fg)

    # EXECUTED stamp – top-right, rotated ~8 degrees, red
    try:
        f_stamp = ImageFont.truetype(FONT_BOLD, 160)
    except Exception:
        f_stamp = ImageFont.load_default()
    stamp_w, stamp_h = 700, 200
    stamp_img = Image.new("RGBA", (stamp_w, stamp_h), (0, 0, 0, 0))
    sd = ImageDraw.Draw(stamp_img)
    sd.text((20, 20), "EXECUTED", font=f_stamp, fill=(190, 30, 30, 180))
    rotated = stamp_img.rotate(-8, expand=True)
    img.paste(rotated, (1650, 120), rotated)

    tmp3 = _save_page(img)

    for tmp in [tmp1, tmp2, tmp3]:
        pdf.add_page()
        pdf.image(tmp, x=0, y=0, w=210, h=297)
        os.unlink(tmp)

    pdf.output(os.path.join(OUT_DIR, "contract_007_legacy_renewal_scanned.pdf"))
    print("Created contract_007_legacy_renewal_scanned.pdf")


# ---------------------------------------------------------------------------
# CONTRACT 008 – Data License Scanned  (4 pages, IMAGE-BASED)
# ---------------------------------------------------------------------------
def contract_008():
    pdf = FPDF(orientation="P", unit="mm", format="A4")
    pdf.set_auto_page_break(False)

    fr, fb, fs = _fonts(44, 52, 36)
    fg = (28, 28, 28)
    BG = (236, 231, 219)  # #ECE7DB

    def page_with_initials(add_initials=False):
        img, draw = _make_aged_page(BG)
        if add_initials:
            try:
                fi = ImageFont.truetype(FONT_BOLD, 52)
            except Exception:
                fi = ImageFont.load_default()
            draw.text((2340, 1750), "JR", font=fi, fill=(80, 80, 80))
        return img, draw

    # ── PAGE 1 ──────────────────────────────────────────────────────────────
    img, draw = page_with_initials(False)

    # CONFIDENTIAL diagonal stamp
    try:
        f_conf = ImageFont.truetype(FONT_BOLD, 130)
    except Exception:
        f_conf = ImageFont.load_default()
    conf_img = Image.new("RGBA", (900, 200), (0, 0, 0, 0))
    cd = ImageDraw.Draw(conf_img)
    cd.text((20, 20), "CONFIDENTIAL", font=f_conf, fill=(200, 60, 60, 90))
    rotated_conf = conf_img.rotate(15, expand=True)
    img.paste(rotated_conf, (800, 80), rotated_conf)

    draw.line([(180, 220), (2300, 220)], fill=(160, 150, 140), width=4)
    y = 270
    draw.text((1240, y), "DATA LICENSE AGREEMENT", font=fb, fill=fg, anchor="mm")
    y += 80
    draw.line([(180, y), (2300, y)], fill=(160, 150, 140), width=2)
    y += 40

    for line in [
        "License No.: CIS-MDL-2022-0031",
        "Effective Date: March 1, 2022",
        "License Term: Two (2) Years",
    ]:
        draw.text((210, y), line, font=fr, fill=fg)
        y += 62
    y += 30

    draw.text((210, y), "PARTIES", font=fb, fill=fg)
    y += 72
    draw.text((210, y), "LICENSOR:  VendorH Systems Corp.", font=fr, fill=fg)
    y += 58
    draw.text(
        (210, y),
        "           1600 Market Street, Suite 3200, Philadelphia, PA 19103",
        font=fs,
        fill=fg,
    )
    y += 54
    draw.text((210, y), "LICENSEE:  Nexacore Data Services LLC", font=fr, fill=fg)
    y += 58
    draw.text(
        (210, y), "           100 Innovation Drive, Boston, MA 02101", font=fs, fill=fg
    )
    y += 80

    draw.text((210, y), "RECITALS", font=fb, fill=fg)
    y += 72
    for line in [
        "WHEREAS, Licensor is in the business of collecting, compiling, and licensing",
        "consumer data, including demographic, behavioral, and contact information for",
        "United States and Canadian consumers;",
        "",
        "WHEREAS, Licensee desires to obtain a license to access and use certain of",
        "Licensor's consumer data assets for permitted business purposes as set forth herein;",
        "",
        "WHEREAS, the parties desire to set forth the terms and conditions under which",
        "Licensor will grant such license to Licensee;",
        "",
        "NOW, THEREFORE, in consideration of the mutual covenants and agreements set forth",
        "herein, the parties agree as follows:",
    ]:
        draw.text((210, y), line, font=fr, fill=fg)
        y += 56

    tmp1 = _save_page(img)

    # ── PAGE 2 ──────────────────────────────────────────────────────────────
    img, draw = page_with_initials(True)
    draw.line([(180, 200), (2300, 200)], fill=(160, 150, 140), width=3)
    y = 250
    draw.text(
        (210, y), "Section 1.  LICENSE GRANT AND PERMITTED USES", font=fb, fill=fg
    )
    y += 72
    for line in [
        "Subject to the terms and conditions of this Agreement, Licensor hereby grants",
        "to Licensee a non-exclusive, non-transferable, limited license to access, use,",
        "and incorporate the Licensed Data solely for the following permitted purposes:",
        "",
        "  (a) MEDIA ACTIVATION: Use of Licensed Data to build and activate audience",
        "      segments across digital and traditional media channels for Licensee's",
        "      direct clients, provided that individual-level data is not exposed.",
        "",
        "  (b) CONSUMER ANALYTICS: Internal analytical modeling, segmentation, and",
        "      insights generation to support Licensee's data products and services.",
        "",
        "  (c) AUDIENCE SEGMENTATION: Creation of proprietary audience taxonomies and",
        "      lookalike models derived from Licensed Data for campaign targeting.",
        "",
        "  (d) MEASUREMENT: Attribution modeling and campaign effectiveness measurement",
        "      using Licensed Data as a reference set, provided results are aggregated.",
    ]:
        draw.text((210, y), line, font=fr, fill=fg)
        y += 56
    y += 30

    draw.text((210, y), "Section 2.  GEOGRAPHIC RESTRICTIONS", font=fb, fill=fg)
    y += 72
    for line in [
        "The license granted herein is restricted to use of Licensed Data for consumers",
        "located in North America (United States and Canada) only. Licensee shall not",
        "use the Licensed Data in connection with any activities targeting consumers",
        "located outside of North America without obtaining a separate written license",
        "from Licensor. Licensee shall implement technical and organizational measures",
        "to ensure compliance with this geographic restriction.",
    ]:
        draw.text((210, y), line, font=fr, fill=fg)
        y += 56
    y += 30

    draw.text((210, y), "Section 3.  LICENSED DATA DESCRIPTION", font=fb, fill=fg)
    y += 72
    data_rows = [
        ("Consumer File:", "U.S. and Canadian consumer identity, ~310 million records"),
        (
            "Data Elements:",
            "Full name, postal address, email (hashed), phone (hashed),",
        ),
        ("", "age range, gender, household income tier, homeowner flag"),
        ("Update Cadence:", "Full monthly refresh + weekly incremental delta files"),
        ("Delivery Format:", "Parquet or CSV (Licensee's choice), AES-256 encrypted"),
        (
            "Delivery Method:",
            "Cloud storage bucket (AWS S3 or GCS), designated by Licensee",
        ),
        ("History:", "24-month rolling lookback included in base license"),
    ]
    for label, value in data_rows:
        if label:
            draw.text((210, y), label, font=fb, fill=fg)
            draw.text((750, y), value, font=fr, fill=fg)
        else:
            draw.text((750, y), value, font=fr, fill=fg)
        y += 58

    tmp2 = _save_page(img)

    # ── PAGE 3 ──────────────────────────────────────────────────────────────
    img, draw = page_with_initials(True)
    draw.line([(180, 200), (2300, 200)], fill=(160, 150, 140), width=3)
    y = 250

    draw.text(
        (210, y), "Section 4.  RESTRICTIONS AND PROHIBITED USES", font=fb, fill=fg
    )
    y += 72
    for line in [
        "Licensee expressly agrees NOT to use the Licensed Data for the following:",
        "",
        "  (a) NO RAW DATA RESALE: Licensee shall not sell, sublicense, rent, transfer,",
        "      or otherwise make available the Licensed Data or any substantial derivative",
        "      thereof to any third party in raw or lightly processed form.",
        "",
        "  (b) NO GOVERNMENT USE: Licensee shall not use the Licensed Data in connection",
        "      with any government contract, law enforcement cooperation, immigration",
        "      enforcement, or national security application.",
        "",
        "  (c) NO FINANCIAL DECISIONING: Licensee shall not use the Licensed Data as a",
        "      primary factor in any credit, insurance, employment, housing, or other",
        "      decision subject to the Fair Credit Reporting Act (FCRA) or similar law.",
    ]:
        draw.text((210, y), line, font=fr, fill=fg)
        y += 56
    y += 30

    draw.text((210, y), "Section 5.  LICENSE FEES AND PAYMENT TERMS", font=fb, fill=fg)
    y += 72
    for line in [
        "Annual License Fee:   $320,000.00 per year",
        "Payment Schedule:     Quarterly installments of $80,000.00 each",
        "Due Dates:            March 1, June 1, September 1, December 1",
        "Payment Terms:        Net 30 days from invoice date",
        "Late Fee:             1.5% per month on overdue balances",
        "Overage Rate:         $0.003 per record above 310M base universe",
    ]:
        draw.text((210, y), line, font=fr, fill=fg)
        y += 62
    y += 30

    draw.text((210, y), "Section 6.  TERM", font=fb, fill=fg)
    y += 72
    for line in [
        "This Agreement shall commence on March 1, 2022 and continue for a period of",
        "two (2) years through February 28, 2024, unless earlier terminated as provided",
        "herein. Following the initial term, this Agreement shall automatically renew",
        "for successive one (1) year periods unless either party provides ninety (90)",
        "days prior written notice of non-renewal.",
    ]:
        draw.text((210, y), line, font=fr, fill=fg)
        y += 56

    tmp3 = _save_page(img)

    # ── PAGE 4 ──────────────────────────────────────────────────────────────
    img, draw = page_with_initials(False)
    draw.line([(180, 200), (2300, 200)], fill=(160, 150, 140), width=3)
    y = 250

    draw.text((210, y), "Section 7.  TERMINATION", font=fb, fill=fg)
    y += 72
    for line in [
        "Either party may terminate this Agreement upon ninety (90) days prior written",
        "notice to the other party. Either party may terminate immediately upon written",
        "notice if the other party materially breaches this Agreement and fails to cure",
        "such breach within thirty (30) days of written notice. Upon termination,",
        "Licensee shall promptly return or certifiably destroy all Licensed Data.",
    ]:
        draw.text((210, y), line, font=fr, fill=fg)
        y += 56
    y += 30

    draw.text((210, y), "Section 8.  GOVERNING LAW", font=fb, fill=fg)
    y += 72
    for line in [
        "This Agreement shall be governed by and construed in accordance with the laws",
        "of the State of Delaware, without regard to its conflict of law principles.",
        "Any disputes shall be resolved by binding arbitration in Wilmington, Delaware.",
    ]:
        draw.text((210, y), line, font=fr, fill=fg)
        y += 56
    y += 50

    # Signature block
    draw.text((210, y), "FOR NEXACORE DATA SERVICES LLC:", font=fb, fill=fg)
    draw.text((1300, y), "FOR VENDORH SYSTEMS CORP.:", font=fb, fill=fg)
    y += 80
    draw.text((210, y), "Signature:", font=fr, fill=fg)
    draw.text((1300, y), "Signature:", font=fr, fill=fg)
    y += 50
    _squiggle(draw, 390, y, 320)
    _squiggle(draw, 1480, y, 320)
    y += 70
    draw.text((210, y), "Name:   Jennifer Rawlings", font=fr, fill=fg)
    draw.text((1300, y), "Name:   Thomas Berkley", font=fr, fill=fg)
    y += 62
    draw.text((210, y), "Title:  VP, Data Licensing", font=fr, fill=fg)
    draw.text((1300, y), "Title:  SVP, Strategic Partnerships", font=fr, fill=fg)
    y += 62
    draw.text((210, y), "Date:   February 22, 2022", font=fr, fill=fg)
    draw.text((1300, y), "Date:   February 25, 2022", font=fr, fill=fg)
    y += 100

    # Notary acknowledgment
    draw.line([(180, y), (2300, y)], fill=(120, 110, 100), width=2)
    y += 40
    draw.text((210, y), "NOTARY ACKNOWLEDGMENT", font=fb, fill=fg)
    y += 72
    draw.text((210, y), "STATE OF DELAWARE", font=fr, fill=fg)
    y += 58
    draw.text((210, y), "County of New Castle, ss.", font=fr, fill=fg)
    y += 58
    draw.text(
        (210, y),
        "On this _____ day of _________________, 2022, before me personally appeared",
        font=fr,
        fill=fg,
    )
    y += 58
    draw.text(
        (210, y),
        "Jennifer Rawlings, known to me or satisfactorily proven to be the person",
        font=fr,
        fill=fg,
    )
    y += 58
    draw.text(
        (210, y),
        "described herein and who executed the foregoing instrument.",
        font=fr,
        fill=fg,
    )
    y += 80
    draw.text(
        (210, y), "Notary Public: _________________________________", font=fr, fill=fg
    )
    y += 62
    draw.text(
        (210, y), "My commission expires: _________________________", font=fr, fill=fg
    )

    tmp4 = _save_page(img)

    for tmp in [tmp1, tmp2, tmp3, tmp4]:
        pdf.add_page()
        pdf.image(tmp, x=0, y=0, w=210, h=297)
        os.unlink(tmp)

    pdf.output(os.path.join(OUT_DIR, "contract_008_data_license_scanned.pdf"))
    print("Created contract_008_data_license_scanned.pdf")


# ---------------------------------------------------------------------------
# CONTRACT 009 – Mutual NDA  (2 pages, TEXT-BASED)
# ---------------------------------------------------------------------------
def contract_009():
    pdf = ContractPDF("MUTUAL NON-DISCLOSURE AGREEMENT")
    pdf.alias_nb_pages()

    # ── PAGE 1 ──────────────────────────────────────────────────────────────
    pdf.add_page()
    pdf.title_block(
        "MUTUAL NON-DISCLOSURE AGREEMENT",
        "Nexacore Inc. and VendorI Inc.",
        "Effective Date: April 1, 2025  |  NDA Term: Two (2) Years",
    )
    pdf.body(
        'This Mutual Non-Disclosure Agreement ("Agreement") is entered into as of April 1, 2025 '
        '("Effective Date") by and between Nexacore Inc., a Delaware corporation having its principal '
        'place of business at 100 Innovation Drive, Boston, MA 02101 ("Party A"), and '
        "VendorI Inc., a California corporation having its principal place of business at "
        '450 Digital Avenue, Atlanta, GA 30309 ("Party B"), collectively referred to as '
        'the "Parties." The Parties desire to explore a potential data partnership or licensing '
        'arrangement (the "Proposed Transaction") and may disclose certain proprietary and '
        "confidential information to each other in connection therewith."
    )

    pdf.section(1, "DEFINITION OF CONFIDENTIAL INFORMATION")
    pdf.body(
        '"Confidential Information" means any and all non-public information disclosed by one '
        'Party (the "Disclosing Party") to the other Party (the "Receiving Party"), whether '
        "disclosed orally, in writing, electronically, or by any other means, that is designated "
        "as confidential or proprietary, or that a reasonable person familiar with the Disclosing "
        "Party's business would understand to be confidential given the nature of the information "
        "and the circumstances surrounding its disclosure. Without limiting the foregoing, "
        "Confidential Information includes: (i) business strategies, plans, and projections; "
        "(ii) financial data, pricing, and fee structures; (iii) technical architectures, "
        "algorithms, data schemas, and software; (iv) customer and prospect lists and contact "
        "information; (v) product roadmaps and development plans; (vi) personnel information; "
        "(vii) marketing strategies and competitive analyses; and (viii) any information marked "
        '"Confidential," "Proprietary," or with a similar legend.'
    )

    pdf.section(2, "OBLIGATIONS OF RECEIVING PARTY")
    pdf.body(
        "Each Receiving Party agrees to the following obligations with respect to Confidential Information:"
    )
    pdf.sub(
        "2.1",
        "HOLD IN CONFIDENCE. The Receiving Party shall hold all Confidential Information in "
        "strict confidence and shall use at least the same degree of care to protect it as it uses "
        "to protect its own most sensitive confidential information, but in no event less than "
        "reasonable care.",
    )
    pdf.sub(
        "2.2",
        "LIMIT DISCLOSURE. The Receiving Party shall disclose Confidential Information only to "
        "its employees, contractors, directors, and professional advisors who have a legitimate "
        "need to know such information in connection with the Proposed Transaction and who are "
        "bound by written confidentiality obligations no less restrictive than those set forth herein.",
    )
    pdf.sub(
        "2.3",
        "USE LIMITATION. The Receiving Party shall use Confidential Information solely for the "
        "purpose of evaluating, negotiating, and, if agreed, consummating the Proposed Transaction. "
        "The Receiving Party shall not use Confidential Information for any other purpose, including "
        "to compete with the Disclosing Party or to develop products or services.",
    )
    pdf.sub(
        "2.4",
        "NOTIFICATION OF UNAUTHORIZED DISCLOSURE. The Receiving Party shall promptly notify the "
        "Disclosing Party in writing upon discovery of any actual or suspected unauthorized access "
        "to, disclosure of, or use of Confidential Information, and shall cooperate fully with "
        "the Disclosing Party to investigate and remediate such incident.",
    )
    pdf.sub(
        "2.5",
        "RETURN OR DESTRUCTION ON REQUEST. Upon the Disclosing Party's written request or upon "
        "termination of this Agreement, the Receiving Party shall promptly return all tangible "
        "materials containing Confidential Information or, at the Disclosing Party's election, "
        "certifiably destroy all such materials, and shall provide written certification of "
        "such destruction within ten (10) business days.",
    )

    pdf.section(3, "EXCLUSIONS FROM CONFIDENTIAL INFORMATION")
    pdf.body(
        "The confidentiality obligations of this Agreement shall not apply to information that:"
    )
    pdf.sub(
        "(a)",
        "was known to the Receiving Party prior to its disclosure by the Disclosing Party, "
        "as evidenced by written records predating such disclosure;",
    )
    pdf.sub(
        "(b)",
        "is or becomes generally available to the public through no act or omission of the "
        "Receiving Party or any person to whom the Receiving Party disclosed such information;",
    )
    pdf.sub(
        "(c)",
        "is independently developed by the Receiving Party without reference to or use of "
        "the Disclosing Party's Confidential Information, as evidenced by contemporaneous "
        "written records; or",
    )
    pdf.sub(
        "(d)",
        "is required to be disclosed by applicable law, regulation, judicial or administrative "
        "order, or the rules of any stock exchange, provided that the Receiving Party: (i) gives "
        "the Disclosing Party prompt prior written notice to the extent legally permissible; "
        "(ii) cooperates with the Disclosing Party's efforts to seek a protective order or "
        "other appropriate relief; and (iii) discloses only that portion of the Confidential "
        "Information that is legally required to be disclosed.",
    )

    pdf.section(4, "TERM AND SURVIVAL")
    pdf.body(
        "This Agreement shall be effective as of the Effective Date and shall continue in full "
        'force and effect for a period of two (2) years (the "Term"), unless earlier terminated '
        "by either Party upon thirty (30) days written notice to the other Party. "
        "Notwithstanding the foregoing, the confidentiality obligations of each Party with "
        "respect to Confidential Information disclosed during the Term shall survive the "
        "expiration or termination of this Agreement for a period of three (3) years following "
        "such expiration or termination. Obligations with respect to trade secrets shall "
        "survive for so long as such information constitutes a trade secret under applicable law."
    )

    pdf.section(5, "RETURN OF INFORMATION")
    pdf.body(
        "Within ten (10) business days after the expiration or termination of this Agreement, "
        "or at any time upon the Disclosing Party's written request, the Receiving Party shall "
        "return or certifiably destroy all Confidential Information and all copies, summaries, "
        "notes, analyses, and compilations thereof in its possession or control. The Receiving "
        "Party shall provide written certification of such return or destruction signed by an "
        "authorized representative. Notwithstanding the foregoing, the Receiving Party may "
        "retain one archival copy solely for legal compliance purposes, subject to continuing "
        "confidentiality obligations hereunder."
    )

    # ── PAGE 2 ──────────────────────────────────────────────────────────────
    pdf.add_page()
    pdf.section(6, "NO LICENSE")
    pdf.body(
        "Nothing in this Agreement shall be construed as granting either Party any license, "
        "right, or interest in any Confidential Information, patent, copyright, trademark, "
        "trade secret, or other intellectual property of the other Party, whether by estoppel, "
        "implication, or otherwise. Each Party retains all right, title, and interest in and "
        "to its own Confidential Information and intellectual property. The disclosure of "
        "Confidential Information hereunder does not obligate either Party to enter into "
        "any further agreement or business relationship."
    )

    pdf.section(7, "NO WARRANTY")
    pdf.body(
        'ALL CONFIDENTIAL INFORMATION IS PROVIDED "AS IS" WITHOUT ANY REPRESENTATION OR '
        "WARRANTY, EXPRESS OR IMPLIED, AS TO ITS ACCURACY, COMPLETENESS, FITNESS FOR A "
        "PARTICULAR PURPOSE, OR NON-INFRINGEMENT. THE DISCLOSING PARTY SHALL NOT BE LIABLE "
        "FOR ANY DAMAGES ARISING FROM THE RECEIVING PARTY'S USE OF OR RELIANCE ON "
        "CONFIDENTIAL INFORMATION."
    )

    pdf.section(8, "EQUITABLE RELIEF")
    pdf.body(
        "Each Party acknowledges that a breach or threatened breach of the confidentiality "
        "obligations set forth in this Agreement would cause irreparable harm to the "
        "Disclosing Party for which monetary damages would be an inadequate remedy. "
        "Accordingly, each Party agrees that the Disclosing Party shall be entitled to "
        "seek injunctive relief or other equitable remedy to prevent or restrain any "
        "such breach or threatened breach, without the requirement of posting a bond "
        "or proving actual damages, in addition to any other remedies available at law "
        "or in equity."
    )

    pdf.section(9, "MISCELLANEOUS")
    pdf.bold_sub(
        "9.1",
        "Entire Agreement",
        "This Agreement constitutes the entire agreement between the Parties with respect "
        "to the subject matter hereof and supersedes all prior and contemporaneous "
        "negotiations, representations, and understandings between the Parties.",
    )
    pdf.bold_sub(
        "9.2",
        "Amendment",
        "This Agreement may not be amended, modified, or waived except by a written "
        "instrument signed by authorized representatives of both Parties.",
    )
    pdf.bold_sub(
        "9.3",
        "Governing Law",
        "This Agreement shall be governed by and construed in accordance with the laws "
        "of the State of Delaware, without regard to its conflict of law principles.",
    )
    pdf.bold_sub(
        "9.4",
        "Counterparts",
        "This Agreement may be executed in counterparts, each of which shall be deemed "
        "an original. Electronic signatures shall be deemed valid and binding.",
    )

    # Signature block
    pdf.ln(4)
    pdf.sig_block(
        party_a="NEXACORE INC.",
        party_b="VENDORI INC.",
        name_a="Alex Morgan",
        title_a="EVP, Data & Analytics",
        name_b="Taylor Kim",
        title_b="Chief Business Officer",
    )

    # Notary block
    pdf.ln(4)
    w = pdf.w - pdf.l_margin - pdf.r_margin
    pdf.set_draw_color(100, 100, 100)
    pdf.line(pdf.l_margin, pdf.get_y(), pdf.w - pdf.r_margin, pdf.get_y())
    pdf.ln(4)
    pdf.set_font("dvb", size=10)
    pdf.cell(
        0, 7, "ACKNOWLEDGMENT OF NOTARY PUBLIC", new_x=XPos.LMARGIN, new_y=YPos.NEXT
    )
    pdf.set_font("dv", size=9)
    pdf.multi_cell(
        w,
        6,
        "STATE OF MARYLAND\n"
        "County of Howard, ss.\n\n"
        "On this _____ day of _________________, 2025, before me personally appeared "
        "Alex Morgan, known to me or satisfactorily proven to be the person whose name "
        "is subscribed to the within instrument and acknowledged that she executed the same "
        "for the purposes therein contained. In witness whereof, I hereunto set my hand "
        "and official seal.",
    )
    pdf.ln(6)
    pdf.cell(90, 7, "Notary Public: _______________________________")
    pdf.cell(
        0,
        7,
        "My commission expires: ______________________",
        new_x=XPos.LMARGIN,
        new_y=YPos.NEXT,
    )

    pdf.output(os.path.join(OUT_DIR, "contract_009_mutual_nda.pdf"))
    print("Created contract_009_mutual_nda.pdf")


# ---------------------------------------------------------------------------
# CONTRACT 010 – Amendment No. 3 with Exhibits  (6 pages, TEXT-BASED)
# ---------------------------------------------------------------------------
def contract_010():
    pdf = ContractPDF("AMENDMENT NO. 3 — MASTER DATA SERVICES AGREEMENT")
    pdf.alias_nb_pages()

    # ── PAGE 1 ──────────────────────────────────────────────────────────────
    pdf.add_page()
    pdf.title_block(
        "AMENDMENT NO. 3 TO MASTER DATA SERVICES AGREEMENT",
        "Incorporating Revised Data Schema, Fee Schedule, and Delivery Terms",
        "Nexacore Inc. and VendorJ Partners LLC  |  Agreement No. NXC-QDP-2021-0019",
    )
    pdf.body("Effective Date of Amendment: June 1, 2025")
    pdf.ln(2)

    pdf.set_font("dvb", size=10)
    pdf.cell(0, 6, "RECITALS", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font("dv", size=10)
    for para in [
        'WHEREAS, Nexacore Inc. ("Nexacore") and VendorJ Partners LLC ("VendorJ") entered '
        "into that certain Master Data Services Agreement dated October 1, 2021, bearing Agreement "
        'No. NXC-QDP-2021-0019 (the "Original Agreement");',
        "WHEREAS, the parties previously executed Amendment No. 1 dated March 15, 2022, which "
        "expanded the scope of Licensed Data and revised the initial fee schedule; and Amendment "
        "No. 2 dated January 1, 2024, which updated delivery specifications and added provisions "
        "for real-time API access;",
        "WHEREAS, the parties now desire to further amend the Original Agreement to incorporate "
        "a revised consumer data schema (Exhibit A), an updated fee schedule (Exhibit B), a "
        "revised delivery schedule (Exhibit C), and authorized representative designations "
        "(Exhibit D), effective as of June 1, 2025;",
        "NOW, THEREFORE, in consideration of the mutual covenants herein and other good and "
        "valuable consideration, the sufficiency of which is hereby acknowledged, the parties "
        "agree as follows:",
    ]:
        pdf.body(para)

    pdf.section(1, "BACKGROUND AND PURPOSE")
    pdf.body(
        "The parties have operated under the Original Agreement and prior amendments for "
        "approximately three and one-half years. During this period, VendorJ has substantially "
        "expanded its consumer data universe, enhanced its identity resolution methodology, and "
        "upgraded its technical infrastructure to support higher-volume, higher-frequency delivery "
        "of consumer data assets. The parties have determined that the existing data schema, fee "
        "structure, and delivery specifications no longer accurately reflect the scope and quality "
        "of services being delivered, and that an amendment is necessary to align contractual "
        "terms with current operational reality."
    )
    pdf.body(
        "This Amendment No. 3 makes the following principal modifications: (i) updates the "
        "consumer data schema to reflect VendorJ's version 3.0 record format, including "
        "the addition of hashed identifier fields and a revised segment coding taxonomy; "
        "(ii) revises the fee schedule to reflect per-record pricing, updated overage rates, "
        "and a new annual minimum commitment through December 31, 2027; (iii) updates the "
        "delivery schedule to reflect current file formats, delivery frequencies, and SLA "
        "commitments; and (iv) designates authorized representatives for contract administration "
        "and technical escalation purposes."
    )
    pdf.body(
        "The parties acknowledge that the modifications set forth herein are intended to be "
        "prospective in nature and shall not affect any rights, obligations, or liabilities "
        "that accrued under the Original Agreement or prior amendments prior to the Effective "
        "Date of this Amendment. All outstanding invoices, disputes, and pending Deliverables "
        "under the prior agreement framework shall be resolved pursuant to the terms in effect "
        "at the time such obligations arose."
    )

    pdf.section(2, "AMENDED DEFINITIONS")
    pdf.body(
        "The following definitions in Section 1 of the Original Agreement are hereby amended and restated in their entirety:"
    )
    pdf.bold_sub(
        "2.1",
        '"Consumer Record"',
        "means a single row of data in the Consumer Identity File representing a unique "
        "consumer identity as resolved by VendorJ's version 3.0 identity resolution "
        "algorithm, including all associated data fields specified in Exhibit A to this "
        "Amendment. The definition of Consumer Record is amended to clarify that records "
        "resolved to the same consumer_id_v3 identifier across multiple source files "
        "shall be counted as a single Consumer Record for billing purposes.",
    )
    pdf.bold_sub(
        "2.2",
        '"Delivery SLA"',
        "means the maximum elapsed time between the close of a data period and the "
        "confirmed availability of the corresponding data file in Licensee's designated "
        "cloud storage location, as specified in the Delivery Schedule attached as Exhibit C. "
        "The Delivery SLA supersedes all prior delivery time commitments in the Original "
        "Agreement and prior amendments.",
    )
    pdf.bold_sub(
        "2.3",
        '"Annual Minimum Commitment"',
        "means the minimum annual dollar amount that Nexacore commits to pay VendorJ "
        "for Data File Delivery services during each contract year, as set forth in the "
        "Revised Fee Schedule attached as Exhibit B. If actual fees incurred in a contract "
        "year are less than the Annual Minimum Commitment, Nexacore shall pay the difference "
        "as a shortfall fee within thirty (30) days following year-end reconciliation.",
    )
    pdf.bold_sub(
        "2.4",
        '"Segment Code"',
        "means the eight-character alphanumeric code assigned to each Consumer Record "
        "reflecting VendorJ's version 3.0 consumer segmentation taxonomy, as described "
        "in the Segment Code Reference Guide provided to Nexacore upon execution of this "
        "Amendment. The Segment Code replaces the prior four-character segment_code field "
        "in all data deliveries effective with the first full-file delivery following the "
        "Effective Date of this Amendment.",
    )

    # ── PAGE 2 ──────────────────────────────────────────────────────────────
    pdf.add_page()
    pdf.section(3, "SCOPE MODIFICATIONS")
    pdf.body(
        "The scope of services under the Original Agreement, as modified by prior amendments, "
        "is hereby further modified as follows, effective June 1, 2025:"
    )
    pdf.bold_sub(
        "3.1",
        "Schema Update",
        "VendorJ shall update all data deliveries to conform to the version 3.0 schema "
        "specified in Exhibit A. VendorJ shall provide a sixty (60) day parallel delivery "
        "period during which both the legacy schema and the version 3.0 schema are delivered "
        "simultaneously to facilitate Nexacore's integration testing and cutover.",
    )
    pdf.bold_sub(
        "3.2",
        "Hashed Identifier Fields",
        "Effective with the first delivery following the Effective Date, VendorJ shall "
        "include SHA-256 hashed email and mobile phone fields as specified in Exhibit A. "
        "These fields are provided on a best-efforts basis; records for which VendorJ "
        "does not hold a verified email or mobile phone will have null values in the "
        "corresponding hash fields.",
    )
    pdf.bold_sub(
        "3.3",
        "Daily Incremental File",
        "VendorJ shall commence daily incremental update file delivery within thirty (30) "
        "days of the Effective Date. The incremental file shall contain all records added, "
        "modified, or suppressed since the prior daily delivery, formatted as NDJSON as "
        "specified in Exhibit C.",
    )
    pdf.bold_sub(
        "3.4",
        "Discontinued Services",
        "The following services provided under Amendment No. 2 are hereby discontinued "
        "effective May 31, 2025, and no further fees shall accrue with respect thereto: "
        "(a) legacy XML delivery format; (b) weekly summary email reports; and "
        "(c) the real-time web portal access previously provided at no charge.",
    )

    pdf.set_font("dvb", size=11)
    pdf.cell(
        0,
        8,
        "EXHIBIT A — REVISED DATA SCHEMA SPECIFICATION",
        new_x=XPos.LMARGIN,
        new_y=YPos.NEXT,
    )
    pdf.set_font("dv", size=10)
    pdf.body(
        "The following table specifies the complete version 3.0 data schema for all data "
        "deliveries under this Agreement, effective June 1, 2025. Fields designated "
        '"MODIFIED" reflect changes from the prior schema. Fields designated "NEW" are '
        'additions. Fields designated "UNCHANGED" retain their prior specifications.'
    )
    cols_a = ["Field Name", "Data Type", "Max Length", "Description", "Change Type"]
    widths_a = [44, 20, 22, 55, 25]
    rows_a = [
        [
            "consumer_id_v3",
            "UUID",
            "36",
            "Primary consumer identifier v3.0",
            "MODIFIED",
        ],
        [
            "full_name_normalized",
            "VARCHAR",
            "150",
            "Standardized full name field",
            "MODIFIED",
        ],
        ["email_address_hashed", "CHAR", "64", "SHA-256 hash of email address", "NEW"],
        [
            "mobile_phone_hash",
            "CHAR",
            "64",
            "SHA-256 hash of mobile phone number",
            "NEW",
        ],
        ["address_line1", "VARCHAR", "200", "Primary street address", "UNCHANGED"],
        ["city", "VARCHAR", "100", "City name", "UNCHANGED"],
        ["state_code", "CHAR", "2", "USPS two-letter state abbreviation", "UNCHANGED"],
        ["zip_code", "CHAR", "10", "ZIP+4 format (NNNNN-NNNN)", "UNCHANGED"],
        [
            "segment_code",
            "CHAR",
            "8",
            "Consumer segment code v3.0 taxonomy",
            "MODIFIED",
        ],
        [
            "last_verified_date",
            "DATE",
            "—",
            "Date of last NCOA/CASS verification",
            "NEW",
        ],
    ]
    pdf.table_simple(cols_a, rows_a, widths_a, row_height=8)

    # ── PAGE 3 ──────────────────────────────────────────────────────────────
    pdf.add_page()
    pdf.set_font("dvb", size=11)
    pdf.cell(
        0, 8, "EXHIBIT B — REVISED FEE SCHEDULE", new_x=XPos.LMARGIN, new_y=YPos.NEXT
    )
    pdf.set_font("dv", size=10)
    pdf.body(
        "The following fee schedule replaces all prior fee schedules in the Original Agreement "
        "and amendments, effective June 1, 2025. All fees are in United States dollars. "
        "Per-record fees are calculated based on the count of Consumer Records delivered "
        "in each calendar month as confirmed in the monthly delivery reconciliation report."
    )
    cols_b = ["Service Component", "Unit", "Rate", "Notes"]
    widths_b = [58, 38, 28, 42]
    rows_b = [
        [
            "Data File Delivery — Base",
            "Per record delivered",
            "$0.0013",
            "First 100M records/month",
        ],
        [
            "Data File Delivery — Overage",
            "Per record delivered",
            "$0.0010",
            "Records above 100M/month",
        ],
        [
            "Identity Resolution API",
            "Per API call",
            "$0.0008",
            "Real-time; 99.9% uptime SLA",
        ],
        [
            "Batch Match Service",
            "Per record processed",
            "$0.0005",
            "Turnaround <= 48 hours",
        ],
        [
            "Annual Minimum Commitment",
            "Per contract year",
            "$550,000",
            "Through December 31, 2027",
        ],
        [
            "Implementation / Onboarding",
            "One-time",
            "$45,000",
            "Due within 30 days of execution",
        ],
    ]
    pdf.table_simple(cols_b, rows_b, widths_b, row_height=9)
    pdf.body(
        "PAYMENT TERMS: VendorJ shall invoice Nexacore monthly in arrears for all usage-based "
        "fees. The Annual Minimum Commitment shall be invoiced in equal quarterly installments of "
        "$137,500 due on the first business day of each calendar quarter. The Implementation / "
        "Onboarding fee shall be invoiced upon execution of this Amendment and due within thirty "
        "(30) days of invoice. All invoices are payable Net 30 days from receipt."
    )
    pdf.body(
        "YEAR-END RECONCILIATION: Within thirty (30) days following each December 31, VendorJ "
        "shall prepare an annual reconciliation statement comparing actual fees incurred against "
        "the Annual Minimum Commitment. If actual fees are less than the Annual Minimum Commitment, "
        "Nexacore shall pay the shortfall with the first quarterly installment of the following year. "
        "If actual fees exceed the Annual Minimum Commitment, no adjustment is required."
    )

    pdf.section(5, "EXHIBIT B — ADDITIONAL FEE TERMS")
    pdf.bold_sub(
        "5.1",
        "Disputed Invoices",
        "Nexacore shall notify VendorJ in writing of any disputed invoice within fifteen (15) "
        "days of receipt. Disputes shall be resolved within thirty (30) days of notice.",
    )
    pdf.bold_sub(
        "5.2",
        "Taxes",
        "All fees are exclusive of applicable taxes. Nexacore shall be responsible for all sales, "
        "use, and similar taxes on amounts paid under this Agreement.",
    )
    pdf.bold_sub(
        "5.3",
        "Fee Adjustment",
        "Beginning January 1, 2027, per-record fees may be adjusted annually by mutual written "
        "agreement not to exceed five percent (5%) in any calendar year.",
    )

    # ── PAGE 4 ──────────────────────────────────────────────────────────────
    pdf.add_page()
    pdf.set_font("dvb", size=11)
    pdf.cell(
        0,
        8,
        "EXHIBIT C — UPDATED DELIVERY SCHEDULE",
        new_x=XPos.LMARGIN,
        new_y=YPos.NEXT,
    )
    pdf.set_font("dv", size=10)
    pdf.body(
        "The following delivery schedule replaces all prior delivery specifications in the "
        "Original Agreement and amendments, effective June 1, 2025. All times are Eastern Time."
    )
    cols_c = ["Dataset", "File Format", "Frequency", "SLA", "Notify Contact"]
    widths_c = [44, 30, 26, 32, 34]
    rows_c = [
        [
            "Consumer Identity File",
            "Parquet",
            "Monthly",
            "3rd business day",
            "dataops@nexacore.com",
        ],
        [
            "Incremental Update File",
            "JSON / NDJSON",
            "Daily",
            "6:00 AM ET",
            "dataops@nexacore.com",
        ],
        [
            "Household Linkage File",
            "CSV",
            "Bi-weekly",
            "Monday 8:00 AM ET",
            "analytics@nexacore.com",
        ],
        [
            "Segment Refresh File",
            "CSV",
            "Weekly",
            "Friday midnight ET",
            "analytics@nexacore.com",
        ],
        [
            "Quality Assurance Report",
            "PDF",
            "Monthly",
            "5th business day",
            "contracts@nexacore.com",
        ],
    ]
    pdf.table_simple(cols_c, rows_c, widths_c, row_height=9)

    pdf.body(
        "SLA REMEDIES: If VendorJ fails to deliver any file by the applicable SLA deadline, "
        "Nexacore shall be entitled to a service credit equal to five percent (5%) of the monthly "
        "Data File Delivery fees for each occurrence. Credits shall be applied to the following "
        "month's invoice. Cumulative credits in any month shall not exceed twenty-five percent "
        "(25%) of that month's invoice. Credits are Nexacore's sole remedy for SLA failures "
        "unless such failures constitute a material breach of this Agreement."
    )

    pdf.section(7, "REPRESENTATIONS AND WARRANTIES")
    pdf.bold_sub(
        "7.1",
        "Mutual Representations",
        "Each party represents and warrants that: (a) it is duly organized and in good standing "
        "under the laws of its jurisdiction of formation; (b) it has full power and authority to "
        "execute and perform this Amendment; (c) this Amendment constitutes the legal, valid, and "
        "binding obligation of such party; and (d) execution of this Amendment does not conflict "
        "with any other agreement or obligation to which such party is a party.",
    )
    pdf.bold_sub(
        "7.2",
        "VendorJ Data Warranties",
        "VendorJ represents and warrants that: (a) all data delivered under this Amendment "
        "shall conform to the specifications in Exhibit A; (b) VendorJ has the right to "
        "license and deliver such data; (c) the data does not infringe any third-party "
        "intellectual property right; and (d) VendorJ's data collection practices comply "
        "with CCPA, CAN-SPAM, and all other applicable privacy laws.",
    )
    pdf.bold_sub(
        "7.3",
        "Disclaimer",
        "EXCEPT AS EXPRESSLY SET FORTH IN THIS AMENDMENT, ALL DATA AND SERVICES ARE PROVIDED "
        '"AS IS" WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED.',
    )

    # ── PAGE 5 ──────────────────────────────────────────────────────────────
    pdf.add_page()
    pdf.section(8, "CONDITIONS PRECEDENT")
    pdf.body(
        "The parties' obligations with respect to the version 3.0 schema deliveries and the "
        "associated modified fees shall not take effect until the following conditions precedent "
        "have been satisfied, which the parties agree to use commercially reasonable efforts "
        "to satisfy within sixty (60) days of the Effective Date:"
    )
    pdf.sub(
        "8.1",
        "VendorJ shall provide Nexacore with a version 3.0 schema test data set "
        "containing no fewer than ten million (10,000,000) representative records, spanning all "
        "change types identified in Exhibit A (MODIFIED, NEW, and UNCHANGED fields).",
    )
    pdf.sub(
        "8.2",
        "Nexacore's data engineering team shall complete integration testing of the "
        "version 3.0 schema and provide written confirmation of successful ingestion, validation, "
        "and downstream processing of all test data within thirty (30) days of receipt.",
    )
    pdf.sub(
        "8.3",
        "The parties shall conduct a joint technical review meeting to validate "
        "schema conformance, discuss any identified issues, and agree on any necessary "
        "schema adjustments prior to production deployment.",
    )
    pdf.sub(
        "8.4",
        "VendorJ shall provide written certification that its production data "
        "pipeline has been updated to generate version 3.0 formatted output and that all "
        "quality assurance testing has been completed with a pass rate of not less than "
        "ninety-nine percent (99%) for each field specified in Exhibit A.",
    )

    pdf.section(9, "TRANSITION PROVISIONS")
    pdf.body(
        "To facilitate a smooth transition to the version 3.0 schema and updated delivery "
        "specifications, the parties agree to the following transition provisions:"
    )
    pdf.sub(
        "9.1",
        "PARALLEL DELIVERY PERIOD: For a period of sixty (60) days following the "
        "satisfaction of all Conditions Precedent set forth in Section 8, VendorJ shall "
        "deliver both the legacy schema format and the version 3.0 schema format for each "
        "scheduled delivery. During this period, Nexacore shall be billed at the legacy fee "
        "schedule. Upon Nexacore's written confirmation of successful integration, the version "
        "3.0 schema shall become the sole delivery format and the revised fee schedule in "
        "Exhibit B shall take effect.",
    )
    pdf.sub(
        "9.2",
        "LEGACY SCHEMA SUNSET: At the conclusion of the parallel delivery period, "
        "VendorJ shall discontinue generation of the legacy schema format. Nexacore "
        "acknowledges that it bears sole responsibility for ensuring its downstream systems "
        "are updated to process the version 3.0 schema prior to the legacy schema sunset date.",
    )
    pdf.sub(
        "9.3",
        "TRANSITION SUPPORT: VendorJ shall make available up to forty (40) "
        "hours of technical support from its data engineering team during the parallel delivery "
        "and integration testing period at no additional charge. Additional support hours, "
        "if required, shall be available at VendorJ's standard rate of $195 per hour.",
    )

    pdf.section(10, "TERMINATION RIGHTS FOR SCHEMA FAILURE")
    pdf.body(
        "In recognition of the material nature of the data schema to Nexacore's downstream "
        "operations, the parties agree to the following termination rights specific to "
        "schema quality failures:"
    )
    pdf.sub(
        "10.1",
        'SCHEMA ERROR THRESHOLD: A "Schema Error" means any record in a delivered '
        "file that fails to conform to the specifications in Exhibit A for any field designated "
        "as MODIFIED or NEW, including null values in non-nullable fields, values exceeding "
        "maximum lengths, or values not conforming to specified data types or enumerated values.",
    )
    pdf.sub(
        "10.2",
        "TERMINATION TRIGGER: If the Schema Error rate, calculated as the percentage "
        "of records in a monthly Consumer Identity File delivery containing one or more Schema "
        "Errors, exceeds two percent (2%) for three (3) consecutive monthly deliveries, either "
        "party may terminate this Agreement upon thirty (30) days written notice, without "
        "payment of any termination penalty or wind-down fee.",
    )
    pdf.sub(
        "10.3",
        "ERROR RATE MEASUREMENT: Schema Error rates shall be calculated by VendorJ "
        "and reported in the Quality Assurance Report delivered pursuant to Exhibit C. Nexacore "
        "shall have the right to audit VendorJ's error rate calculations upon reasonable "
        "written notice.",
    )

    # ── PAGE 6 ──────────────────────────────────────────────────────────────
    pdf.add_page()
    pdf.section(11, "GENERAL PROVISIONS")
    pdf.bold_sub(
        "11.1",
        "Effect of Amendment",
        "Except as expressly modified by this Amendment, all terms and conditions of the "
        "Original Agreement and prior amendments remain in full force and effect. In the "
        "event of any conflict between this Amendment and the Original Agreement or any "
        "prior amendment, this Amendment shall control.",
    )
    pdf.bold_sub(
        "11.2",
        "Integration",
        "This Amendment, together with the Original Agreement and Amendments No. 1 and No. 2, "
        "constitutes the entire agreement between the parties with respect to the subject "
        "matter of the Original Agreement and supersedes all prior negotiations and "
        "representations with respect thereto.",
    )
    pdf.bold_sub(
        "11.3",
        "Counterparts",
        "This Amendment may be executed in counterparts, each of which shall be deemed "
        "an original, and all of which shall together constitute one and the same instrument. "
        "Electronic and digital signatures shall be deemed valid and binding.",
    )
    pdf.bold_sub(
        "11.4",
        "Governing Law",
        "This Amendment shall be governed by the laws of the State of Maryland, consistent "
        "with the governing law provision of the Original Agreement.",
    )

    pdf.sig_block(
        party_a="NEXACORE INC.",
        party_b="VENDORJ PARTNERS LLC",
        name_a="Alex Morgan",
        title_a="EVP, Data & Analytics",
        name_b="Riley Johnson",
        title_b="Chief Executive Officer",
    )

    # EXHIBIT D — full signature page
    pdf.ln(4)
    pdf.set_draw_color(80, 80, 80)
    pdf.line(pdf.l_margin, pdf.get_y(), pdf.w - pdf.r_margin, pdf.get_y())
    pdf.ln(4)
    pdf.set_font("dvb", size=11)
    pdf.cell(
        0,
        7,
        "EXHIBIT D — SIGNATURE PAGE FOR AUTHORIZED REPRESENTATIVES",
        new_x=XPos.LMARGIN,
        new_y=YPos.NEXT,
    )
    pdf.set_font("dv", size=9)
    pdf.body(
        "The following authorized representatives are designated by each party for purposes "
        "of contract administration, amendment execution, and technical escalation under "
        "the Master Data Services Agreement No. NXC-QDP-2021-0019, as amended."
    )
    w = pdf.w - pdf.l_margin - pdf.r_margin
    col = w / 2 - 4

    def sig_row(label_a, val_a, label_b, val_b):
        pdf.set_font("dvb", size=9)
        lw = pdf.get_string_width("Signature:  ")
        pdf.cell(lw, 7, label_a)
        pdf.set_font("dv", size=9)
        pdf.cell(col - lw, 7, val_a)
        pdf.cell(8, 7, "")
        pdf.set_font("dvb", size=9)
        pdf.cell(lw, 7, label_b)
        pdf.set_font("dv", size=9)
        pdf.cell(col - lw, 7, val_b, new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    pdf.ln(2)
    pdf.set_font("dvb", size=10)
    pdf.cell(col, 7, "FOR NEXACORE INC.:")
    pdf.cell(8, 7, "")
    pdf.cell(col, 7, "FOR VENDORJ PARTNERS LLC:", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    sig_row("Name:", "Alex Morgan", "Name:", "Riley Johnson")
    sig_row("Title:", "EVP, Data & Analytics", "Title:", "Chief Executive Officer")
    sig_row("Date:", "______________________", "Date:", "______________________")
    sig_row(
        "Signature:", "______________________", "Signature:", "______________________"
    )
    pdf.ln(6)

    pdf.set_font("dvb", size=10)
    pdf.cell(0, 7, "WITNESS:", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    sig_row("Name:", "______________________", "Signature:", "______________________")
    sig_row("Date:", "______________________", "", "")

    pdf.output(os.path.join(OUT_DIR, "contract_010_amendment_exhibits.pdf"))
    print("Created contract_010_amendment_exhibits.pdf")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    os.makedirs(OUT_DIR, exist_ok=True)
    contract_006()
    contract_007()
    contract_008()
    contract_009()
    contract_010()
    print("\nAll 5 contracts (006-010) generated successfully.")
