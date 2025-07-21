-- Create database and schema
--CREATE OR REPLACE DATABASE sales_intelligence;
--CREATE OR REPLACE SCHEMA sales_intelligence.data;
--CREATE OR REPLACE WAREHOUSE sales_intelligence_wh;

USE DATABASE PNP;
USE SCHEMA ETREMBLAY;

-- Create tables for sales data
CREATE TABLE sales_conversations (
    conversation_id VARCHAR,
    transcript_text TEXT,
    customer_name VARCHAR,
    deal_stage VARCHAR,
    sales_rep VARCHAR,
    conversation_date TIMESTAMP,
    deal_value FLOAT,
    product_line VARCHAR
);

CREATE TABLE sales_metrics (
    deal_id VARCHAR,
    customer_name VARCHAR,
    deal_value FLOAT,
    close_date DATE,
    sales_stage VARCHAR,
    win_status BOOLEAN,
    sales_rep VARCHAR,
    product_line VARCHAR
);

CREATE TABLE sales_metrics (
    deal_id VARCHAR,
    customer_name VARCHAR,
    deal_value FLOAT,
    close_date DATE,
    sales_stage VARCHAR,
    win_status BOOLEAN,
    sales_rep VARCHAR,
    product_line VARCHAR
);


```sql
CREATE OR REPLACE TABLE emails_webinar_202508 ( (
  id             NUMBER AUTOINCREMENT
                  COMMENT 'Surrogate primary key for each email record',
  
  message_id     VARCHAR(255)
                  COMMENT 'Unique message identifier assigned by the mail provider',
  
  thread_id      VARCHAR(255)
                  COMMENT 'Identifier grouping related messages into a conversation thread',
  
  from_address   VARCHAR(320)
                  COMMENT 'Email address of the sender (max 320 chars per RFC)',
  
  to_addresses   VARCHAR(1000)
                  COMMENT 'Comma‑separated list of primary recipient email addresses',
  
  cc_addresses   VARCHAR(1000)
                  COMMENT 'Comma‑separated list of CC recipient email addresses',
  
  bcc_addresses  VARCHAR(1000)
                  COMMENT 'Comma‑separated list of BCC recipient email addresses',
  
  subject        VARCHAR(1000)
                  COMMENT 'Subject line of the email message',
  
  body           STRING
                  COMMENT 'Full message body (plain‑text or HTML)',
  
  sent_at        TIMESTAMP_NTZ
                  COMMENT 'When the email was sent (no time zone stored)',
  
  received_at    TIMESTAMP_NTZ
                  COMMENT 'When the email was received or ingested',
  
  is_read        BOOLEAN DEFAULT FALSE
                  COMMENT 'Flag indicating whether the user has read this email',
  
  created_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                  COMMENT 'Record insertion timestamp'
);
```
**Column Descriptions**

* **`id`**: Auto‑incrementing surrogate key.
* **`message_id`**: Provider’s unique message ID (e.g. Gmail/RFC‑822 Message‑ID).
* **`thread_id`**: Conversation/thread grouping key.
* **`from_address`**: Sender’s email address.
* **`to_addresses`**, **`cc_addresses`**, **`bcc_addresses`**: Comma‑separated recipient lists.
* **`subject`**: Email subject line.
* **`body`**: The full email payload (text or HTML).
* **`sent_at`** / **`received_at`**: Timestamps for send and receive events.
* **`is_read`**: Read/unread status flag.
* **`created_at`**: When this row was inserted into the table.



-- First, let's insert data into sales_conversations
INSERT INTO sales_conversations
(conversation_id, transcript_text, customer_name, deal_stage, sales_rep, conversation_date, deal_value, product_line)
VALUES
('CONV001', 'Initial discovery call with TechCorp Inc''s IT Director and Solutions Architect. Client showed strong interest in our enterprise solution features, particularly the automated workflow capabilities. The main discussion centered around integration timeline and complexity. They currently use Legacy System X for their core operations and expressed concerns about potential disruption during migration. The team asked detailed questions about API compatibility and data migration tools.

Action items include providing a detailed integration timeline document, scheduling a technical deep-dive with their infrastructure team, and sharing case studies of similar Legacy System X migrations. The client mentioned a Q2 budget allocation for digital transformation initiatives. Overall, it was a positive engagement with clear next steps.', 'TechCorp Inc', 'Discovery', 'Sarah Johnson', '2024-01-15 10:30:00', 75000, 'Enterprise Suite'),

('CONV002', 'Follow-up call with SmallBiz Solutions'' Operations Manager and Finance Director. The primary focus was on pricing structure and ROI timeline. They compared our Basic Package pricing with Competitor Y''s small business offering. Key discussion points included monthly vs. annual billing options, user license limitations, and potential cost savings from process automation.

The client requested a detailed ROI analysis focusing on time saved in daily operations, resource allocation improvements, and projected efficiency gains. Budget constraints were clearly communicated, with a maximum budget of $30K for this year. They showed interest in starting with the basic package with room for a potential upgrade in Q4. Next steps include providing a competitive analysis and a customized ROI calculator by next week.', 'SmallBiz Solutions', 'Negotiation', 'Mike Chen', '2024-01-16 14:45:00', 25000, 'Basic Package'),

('CONV003', 'Strategy session with SecureBank Ltd''s CISO and Security Operations team. Extremely positive 90-minute deep dive into our Premium Security package. Customer emphasized immediate need for implementation due to recent industry compliance updates. Our advanced security features, especially multi-factor authentication and encryption protocols, were identified as perfect fits for their requirements. Technical team was particularly impressed with our zero-trust architecture approach and real-time threat monitoring capabilities. They''ve already secured budget approval and have executive buy-in. Compliance documentation is ready for review. Action items include: finalizing implementation timeline, scheduling security audit, and preparing necessary documentation for their risk assessment team. Client ready to move forward with contract discussions.', 'SecureBank Ltd', 'Closing', 'Rachel Torres', '2024-01-17 11:20:00', 150000, 'Premium Security'),

('CONV004', 'Comprehensive discovery call with GrowthStart Up''s CTO and Department Heads. Team of 500+ employees across 3 continents discussed current challenges with their existing solution. Major pain points identified: system crashes during peak usage, limited cross-department reporting capabilities, and poor scalability for remote teams. Deep dive into their current workflow revealed bottlenecks in data sharing and collaboration. Technical requirements gathered for each department. Platform demo focused on scalability features and global team management capabilities. Client particularly interested in our API ecosystem and custom reporting engine. Next steps: schedule department-specific workflow analysis and prepare detailed platform migration plan.', 'GrowthStart Up', 'Discovery', 'Sarah Johnson', '2024-01-18 09:15:00', 100000, 'Enterprise Suite'),

('CONV005', 'In-depth demo session with DataDriven Co''s Analytics team and Business Intelligence managers. Showcase focused on advanced analytics capabilities, custom dashboard creation, and real-time data processing features. Team was particularly impressed with our machine learning integration and predictive analytics models. Competitor comparison requested specifically against Market Leader Z and Innovative Start-up X. Price point falls within their allocated budget range, but team expressed interest in multi-year commitment with corresponding discount structure. Technical questions centered around data warehouse integration and custom visualization capabilities. Action items: prepare detailed competitor feature comparison matrix and draft multi-year pricing proposals with various discount scenarios.', 'DataDriven Co', 'Demo', 'James Wilson', '2024-01-19 13:30:00', 85000, 'Analytics Pro'),

('CONV006', 'Extended technical deep dive with HealthTech Solutions'' IT Security team, Compliance Officer, and System Architects. Four-hour session focused on API infrastructure, data security protocols, and compliance requirements. Team raised specific concerns about HIPAA compliance, data encryption standards, and API rate limiting. Detailed discussion of our security architecture, including: end-to-end encryption, audit logging, and disaster recovery protocols. Client requires extensive documentation on compliance certifications, particularly SOC 2 and HITRUST. Security team performed initial architecture review and requested additional information about: database segregation, backup procedures, and incident response protocols. Follow-up session scheduled with their compliance team next week.', 'HealthTech Solutions', 'Technical Review', 'Rachel Torres', '2024-01-20 15:45:00', 120000, 'Premium Security'),

('CONV007', 'Contract review meeting with LegalEase Corp''s General Counsel, Procurement Director, and IT Manager. Detailed analysis of SLA terms, focusing on uptime guarantees and support response times. Legal team requested specific modifications to liability clauses and data handling agreements. Procurement raised questions about payment terms and service credit structure. Key discussion points included: disaster recovery commitments, data retention policies, and exit clause specifications. IT Manager confirmed technical requirements are met pending final security assessment. Agreement reached on most terms, with only SLA modifications remaining for discussion. Legal team to provide revised contract language by end of week. Overall positive session with clear path to closing.', 'LegalEase Corp', 'Negotiation', 'Mike Chen', '2024-01-21 10:00:00', 95000, 'Enterprise Suite'),

('CONV008', 'Quarterly business review with GlobalTrade Inc''s current implementation team and potential expansion stakeholders. Current implementation in Finance department showcasing strong adoption rates and 40% improvement in processing times. Discussion focused on expanding solution to Operations and HR departments. Users highlighted positive experiences with customer support and platform stability. Challenges identified in current usage: need for additional custom reports and increased automation in workflow processes. Expansion requirements gathered from Operations Director: inventory management integration, supplier portal access, and enhanced tracking capabilities. HR team interested in recruitment and onboarding workflow automation. Next steps: prepare department-specific implementation plans and ROI analysis for expansion.', 'GlobalTrade Inc', 'Expansion', 'James Wilson', '2024-01-22 14:20:00', 45000, 'Basic Package'),

('CONV009', 'Emergency planning session with FastTrack Ltd''s Executive team and Project Managers. Critical need for rapid implementation due to current system failure. Team willing to pay premium for expedited deployment and dedicated support team. Detailed discussion of accelerated implementation timeline and resource requirements. Key requirements: minimal disruption to operations, phased data migration, and emergency support protocols. Technical team confident in meeting aggressive timeline with additional resources. Executive sponsor emphasized importance of going live within 30 days. Immediate next steps: finalize expedited implementation plan, assign dedicated support team, and begin emergency onboarding procedures. Team to reconvene daily for progress updates.', 'FastTrack Ltd', 'Closing', 'Sarah Johnson', '2024-01-23 16:30:00', 180000, 'Premium Security'),

('CONV010', 'Quarterly strategic review with UpgradeNow Corp''s Department Heads and Analytics team. Current implementation meeting basic needs but team requiring more sophisticated analytics capabilities. Deep dive into current usage patterns revealed opportunities for workflow optimization and advanced reporting needs. Users expressed strong satisfaction with platform stability and basic features, but requiring enhanced data visualization and predictive analytics capabilities. Analytics team presented specific requirements: custom dashboard creation, advanced data modeling tools, and integrated BI features. Discussion about upgrade path from current package to Analytics Pro tier. ROI analysis presented showing potential 60% improvement in reporting efficiency. Team to present upgrade proposal to executive committee next month.', 'UpgradeNow Corp', 'Expansion', 'Rachel Torres', '2024-01-24 11:45:00', 65000, 'Analytics Pro');

-- Now, let's insert corresponding data into sales_metrics
INSERT INTO sales_metrics
(deal_id, customer_name, deal_value, close_date, sales_stage, win_status, sales_rep, product_line)
VALUES
('DEAL001', 'TechCorp Inc', 75000, '2024-02-15', 'Closed', true, 'Sarah Johnson', 'Enterprise Suite'),

('DEAL002', 'SmallBiz Solutions', 25000, '2024-02-01', 'Lost', false, 'Mike Chen', 'Basic Package'),

('DEAL003', 'SecureBank Ltd', 150000, '2024-01-30', 'Closed', true, 'Rachel Torres', 'Premium Security'),

('DEAL004', 'GrowthStart Up', 100000, '2024-02-10', 'Pending', false, 'Sarah Johnson', 'Enterprise Suite'),

('DEAL005', 'DataDriven Co', 85000, '2024-02-05', 'Closed', true, 'James Wilson', 'Analytics Pro'),

('DEAL006', 'HealthTech Solutions', 120000, '2024-02-20', 'Pending', false, 'Rachel Torres', 'Premium Security'),

('DEAL007', 'LegalEase Corp', 95000, '2024-01-25', 'Closed', true, 'Mike Chen', 'Enterprise Suite'),

('DEAL008', 'GlobalTrade Inc', 45000, '2024-02-08', 'Closed', true, 'James Wilson', 'Basic Package'),

('DEAL009', 'FastTrack Ltd', 180000, '2024-02-12', 'Closed', true, 'Sarah Johnson', 'Premium Security'),

('DEAL010', 'UpgradeNow Corp', 65000, '2024-02-18', 'Pending', false, 'Rachel Torres', 'Analytics Pro');



INSERT INTO emails_webinar_202508 (
  message_id,
  thread_id,
  from_address,
  to_addresses,
  cc_addresses,
  bcc_addresses,
  subject,
  body,
  sent_at,
  received_at,
  is_read,
  created_at
)
SELECT
  UUID_STRING()                                                                                             AS message_id,
  UUID_STRING()                                                                                             AS thread_id,

  -- customer email
  ARRAY_CONSTRUCT(
    'alice.smith@gmail.com','bob.jones@yahoo.com','carol.lee@outlook.com',
    'dave.wilson@example.com','eve.moore@gmail.com','frank.taylor@yahoo.com',
    'grace.anderson@outlook.com','heidi.brown@example.com',
    'ivan.johnson@gmail.com','judy.white@yahoo.com'
  )[UNIFORM(0,10,RANDOM())]::VARCHAR                                                                         AS from_address,

  'sales@snowbins.ca'                                                                                       AS to_addresses,
  ''                                                                                                        AS cc_addresses,
  ''                                                                                                        AS bcc_addresses,

  /* SUBJECT – always complete */
  'Request for '
    || ARRAY_CONSTRUCT('10 yd³','15 yd³','20 yd³','30 yd³')[UNIFORM(0,4,RANDOM())]::VARCHAR
    || ' '
    || ARRAY_CONSTRUCT(
         'mixed waste','green waste','construction debris','concrete',
         'metal scrap','furniture','yard waste'
       )[UNIFORM(0,7,RANDOM())]::VARCHAR
    || ' container rental'                                                                                   AS subject,

  /* BODY – 80% precise, 20% vague */
  CASE
    WHEN UNIFORM(0,10,RANDOM()) < 8 THEN
      /* precise branch */
      ARRAY_CONSTRUCT('Hello','Hi','Greetings','Dear team')[UNIFORM(0,4,RANDOM())]::VARCHAR
      || ' SnowBins,' || '\n\n'
      || 'I need to rent a '
      || ARRAY_CONSTRUCT('10 yd³','15 yd³','20 yd³','30 yd³')[UNIFORM(0,4,RANDOM())]::VARCHAR
      || ' container for '
      || ARRAY_CONSTRUCT(
           'mixed waste','green waste','construction debris','concrete',
           'metal scrap','furniture','yard waste'
         )[UNIFORM(0,7,RANDOM())]::VARCHAR
      || ', approx ' || TO_VARCHAR(UNIFORM(1,10,RANDOM()))
      || IFF(UNIFORM(0,2,RANDOM())=0,' tons',' yd³')
      || '. Please deliver on '
      /* four date formats */
      || CASE UNIFORM(0,4,RANDOM())
           WHEN 0 THEN TO_CHAR(
                        DATEADD('day', UNIFORM(0,30,RANDOM()), '2025-08-01'::DATE),
                        'YYYY-MM-DD'
                      )
           WHEN 1 THEN TO_CHAR(
                        DATEADD('day', UNIFORM(0,30,RANDOM()), '2025-08-01'::DATE),
                        'Month DD, YYYY'
                      )
           WHEN 2 THEN TO_CHAR(
                        DATEADD('day', UNIFORM(0,30,RANDOM()), '2025-08-01'::DATE),
                        'DD/MM/YYYY'
                      )
           ELSE      TO_CHAR(
                        DATEADD('day', UNIFORM(0,30,RANDOM()), '2025-08-01'::DATE),
                        'DD Mon YYYY'
                      )
         END
      || ' for ' || TO_VARCHAR(UNIFORM(3,14,RANDOM())) || ' days at '
      || TO_VARCHAR(UNIFORM(100,999,RANDOM())) || ' '
      || ARRAY_CONSTRUCT(
           'Maple St','Oak St','Pine Ave','Elm Dr','Cedar Blvd',
           'Sunset Blvd','Lincoln Ave','Adams St','Madison Ave','Jefferson St'
         )[UNIFORM(0,10,RANDOM())]::VARCHAR
      || ', '
      || ARRAY_CONSTRUCT(
           'Los Angeles, CA','San Diego, CA','Sacramento, CA','San Jose, CA',
           'Fresno, CA','Bakersfield, CA','Oakland, CA','San Francisco, CA',
           'Irvine, CA','Riverside, CA'
         )[UNIFORM(0,10,RANDOM())]::VARCHAR
      || '\n\n'
      || ARRAY_CONSTRUCT('Thanks,','Best regards,','Cheers,','Sincerely,')[UNIFORM(0,4,RANDOM())]::VARCHAR
      || '\n'
      || SPLIT_PART(
           ARRAY_CONSTRUCT(
             'alice.smith@gmail.com','bob.jones@yahoo.com','carol.lee@outlook.com',
             'dave.wilson@example.com','eve.moore@gmail.com','frank.taylor@yahoo.com',
             'grace.anderson@outlook.com','heidi.brown@example.com',
             'ivan.johnson@gmail.com','judy.white@yahoo.com'
           )[UNIFORM(0,10,RANDOM())]::VARCHAR,
           '@',1
         )

    ELSE
      /* vague branch */
      ARRAY_CONSTRUCT('Hello','Hi','Greetings')[UNIFORM(0,3,RANDOM())]::VARCHAR
      || ' SnowBins,' || '\n\n'
      || 'I need '
      || ARRAY_CONSTRUCT('some containers','a few bins')[UNIFORM(0,2,RANDOM())]::VARCHAR
      || ' for '
      || ARRAY_CONSTRUCT('green waste','mixed waste','debris')[UNIFORM(0,3,RANDOM())]::VARCHAR
      || ' '
      || ARRAY_CONSTRUCT('end of August','early September')[UNIFORM(0,2,RANDOM())]::VARCHAR
      || ' at my usual location.'
      || '\n\n'
      || ARRAY_CONSTRUCT('Cheers,','Sincerely,')[UNIFORM(0,2,RANDOM())]::VARCHAR
      || '\n'
      || SPLIT_PART(
           ARRAY_CONSTRUCT(
             'alice.smith@gmail.com','bob.jones@yahoo.com','carol.lee@outlook.com',
             'dave.wilson@example.com','eve.moore@gmail.com','frank.taylor@yahoo.com',
             'grace.anderson@outlook.com','heidi.brown@example.com',
             'ivan.johnson@gmail.com','judy.white@yahoo.com'
           )[UNIFORM(0,10,RANDOM())]::VARCHAR,
           '@',1
         )
  END                                                                                                       AS body,
  /* sent_at: random Aug 2025, minus 1–5 days, plus random hour */
  DATEADD(
    'hour',
    UNIFORM(0,23,RANDOM()),
    DATEADD(
      'day',
      -UNIFORM(1,5,RANDOM()),
      DATEADD('day', UNIFORM(0,30,RANDOM()), '2025-08-01'::DATE)
    )
  )                                                                                                         AS sent_at,

  /* received_at: 1–60 minutes later */
  DATEADD('minute', UNIFORM(1,60,RANDOM()), sent_at)                                                        AS received_at,

  /* is_read */
  IFF(UNIFORM(0,2,RANDOM())=0, TRUE, FALSE)                                                                  AS is_read,

  CURRENT_TIMESTAMP()                                                                                       AS created_at
FROM TABLE(GENERATOR(ROWCOUNT => 100));

ALTER TABLE emails_webinar_202508 SET CHANGE_TRACKING = TRUE;


-- Enable change tracking
ALTER TABLE sales_conversations SET CHANGE_TRACKING = TRUE;

-- Create the search service
-- Peut aussi maintenant être fait via l'interfacde de Snowpark
CREATE OR REPLACE CORTEX SEARCH SERVICE sales_conversation_search
  ON transcript_text
  ATTRIBUTES customer_name, deal_stage, sales_rep, product_line, conversation_date, deal_value
  WAREHOUSE = COMPUTE_WH
  TARGET_LAG = '1 minute'
  AS (
    SELECT
        conversation_id,
        transcript_text,
        customer_name,
        deal_stage,
        sales_rep,
        conversation_date,
        deal_value,
        product_line
    FROM sales_conversations
    WHERE conversation_date >= '2024-01-01'  -- Fixed date instead of CURRENT_TIMESTAMP
);

CREATE OR REPLACE PNP.ETREMBLAY.MODELSACE STAGE models
    DIRECTORY = (ENABLE = TRUE);



    
- Donner accès à l'API HERE et spécifier sa  KEY

CREATE OR REPLACE NETWORK RULE here_api_rules  
MODE = EGRESS  
TYPE = HOST_PORT  
VALUE_LIST = ('router.hereapi.com','geocode.search.hereapi.com');

CREATE OR REPLACE SECRET here_api_key  
TYPE = GENERIC_STRING  
SECRET_STRING = 'REMOVED'; 

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION here_api_access_int  
ALLOWED_NETWORK_RULES = (here_api_rules)  
ALLOWED_AUTHENTICATION_SECRETS = (here_api_key)  
ENABLED = TRUE;

GRANT READ ON SECRET here_api_key TO ROLE PNP;

GRANT USAGE ON INTEGRATION here_api_access_int TO ROLE PNP;

--Get Streamlit ID 
SHOW STREAMLITS IN SCHEMA PNP.ETREMBLAY;

ALTER STREAMLIT PNP.ETREMBLAY.V23DZEU56TC6GE2H --Streamlit ID  
SET EXTERNAL_ACCESS_INTEGRATIONS = (here_api_access_int)  
SECRETS = ('here_api_key' = pnp.etremblay.here_api_key);




