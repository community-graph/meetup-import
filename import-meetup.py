import os
import time
import requests
import time
from neo4j.v1 import GraphDatabase, basic_auth

neo4jUrl = os.environ.get('NEO4J_URL',"bolt://localhost")
neo4jUser = os.environ.get('NEO4J_USER',"neo4j")
neo4jPass = os.environ.get('NEO4J_PASSWORD',"test")
meetupKey = os.environ.get('MEETUP_API_KEY',"")

if len(meetupKey) == 0 : 
    raise(Exception("No Meetup API Key configured"))

driver = GraphDatabase.driver(neo4jUrl, auth=basic_auth(neo4jUser, neo4jPass))

session = driver.session()


importGroups = """
UNWIND {json} as g
MERGE (group:Group:Meetup:Container {id:g.id}) 
  ON CREATE SET group.title=g.name,group.text=g.description,group.key=g.urlname,group.country=g.country,group.city=g.city,group.created=g.created,group.link=g.link,group.longitude=g.lon,group.latitude=g.lat,group.members=g.members
SET group.rating=g.rating
FOREACH (organizer IN [o in [g.organizer] WHERE g.organizer IS NOT NULL] |
  MERGE (owner:User:Meetup {id:organizer.member_id}) ON CREATE SET owner.name = organizer.name
  MERGE (owner)-[:CREATED]->(group)
)

FOREACH (t IN g.topics | MERGE (tag:Tag:Meetup {name:t.urlkey}) ON CREATE SET tag.id = t.id, tag.description=t.name MERGE (group)-[:TAGGED]->(tag))
WITH group WHERE (group.title + group.text) =~ "(?is).*(graph|neo4j).*"
SET group:Graph
"""

importMembers = """
MATCH (g:Group {id:{group}})
UNWIND {json} as m
MERGE (user:User:Meetup {id:m.id}) 
  ON CREATE SET user.name=m.name,user.text=m.bio,user.country=m.country,user.city=m.city,user.state=m.state,user.hometown=m.hometown,
  user.created=m.joined,user.latitude=m.lat,user.longitude=m.lon,user.link=m.link,user.groups=m.membership_count,user.picture=m.photo_url
SET user.updated=m.visited
MERGE (user)-[:JOINED]->(g)
FOREACH (t IN m.topics | MERGE (tag:Tag:Meetup {name:t.urlkey}) ON CREATE SET tag.id = t.id, tag.description=t.name MERGE (user)-[:TAGGED]->(tag))
"""

importEvents = """
UNWIND {json} as e
MATCH (g:Group {id:e.group.id})
WITH e,g WHERE (g:Graph OR (e.name + e.description) =~ "(?is).*(graph|neo4j).*")
MERGE (event:Event:Meetup {id:e.id}) 
ON CREATE SET event.title=e.name,event.text=e.description,event.created=e.created,
event.link=e.event_url,event.time=e.time,
event.utc_offset=e.utc_offset,event.duration=e.duration
SET event.updated=e.updated,event.headcount=e.headcount,event.waitlist_count=e.waitlist_count,
event.maybe_rsvp_count=e.maybe_rsvp_count,event.yes_rsvp_count=e.yes_rsvp_count,event.rsvp_limit=e.rsvp_limit,
event.announced=e.announced,event.comment_count=e.comment_count,event.status=e.status,event.rating=e.rating.average,event.ratings=e.rating.count
MERGE (g)-[:CONTAINED]->(event)
FOREACH (o in coalesce(e.event_hosts,[]) |
  MERGE (host:User:Meetup {id:o.member_id}) ON CREATE SET host.name = o.member_name
  MERGE (host)-[:CREATED]->(event)
)

WITH event, e.venue as v
WHERE v IS NOT NULL AND v.id IS NOT NULL
MERGE (venue:Venue:Meetup {id:v.id}) ON CREATE SET venue.name=v.name, venue.longitude=v.lon,venue.latitude=v.lat,venue.country = v.country, venue.city=v.city,venue.address=v.address_1,venue.country_name = v.localized_country_name

MERGE (venue)-[:HOSTED]->(event)
"""

importRsvps = """
UNWIND {json} as r
MATCH (user:User:Meetup {id:r.member.member_id}) 
MATCH (event:Event:Meetup {id:r.event.id})
MERGE (user)-[rsvp:ATTENDED]->(event) 
ON CREATE SET rsvp.comments=r.comments,rsvp.created=r.created
SET rsvp.updated=r.mtime,rsvp.guests=r.guests,rsvp.host=r.host,rsvp.response=r.response
"""

def run_import(type, url, importQuery, params):
    page=0
    hasMore=True
    items=100

    while hasMore == True:
        apiUrl = url + "&key={key}&offset={offset}&page={items}".format(key=meetupKey,offset=page,items=items)

        response = requests.get(apiUrl, headers = {"accept":"application/json"})
        if response.status_code != 200:
            print(response.text)

        rate_remain=int(response.headers['X-RateLimit-Remaining'])
        rate_reset=int(response.headers['X-RateLimit-Reset'])

        json = response.json()
        meta = json['meta']
        results = json.get("results",[])
        hasMore = len(meta.get("next","")) > 0
        if  len(results) > 0:
            p = {"json":results}
            p.update(params)
            result = session.run(importQuery,p)
            print(result.consume().counters)
            page = page + 1

        print(type,"results",len(results),"has_more",hasMore,"quota",rate_remain,"reset (s)",rate_reset,"page",page)
        time.sleep(1)
        if rate_remain <= 0:
            time.sleep(rate_reset) 

tag="neo4j"
groupUrl="https://api.meetup.com/2/groups?topic={tag}&radius=36000&text_format=plain&order=id&omit=contributions,group_photo,approved,join_info,membership_dues,self,similar_groups,sponsors,simple_html_description,welcome_message".format(tag=tag)
run_import("groups",groupUrl,importGroups,{})

result = session.run("MATCH (g:Group:Meetup) RETURN g.id as id, g.key as key")
groups = []
for record in result:
    if record["id"] != None:
        group = record["id"]
        memberUrl="https://api.meetup.com/2/members?group_id={group}&text_format=plain&order=visited&omit=photo,photos".format(group=group)
        run_import("members "+record["key"],memberUrl,importMembers,{"group":group})
        groups += [str(group)]
        
eventUrl="https://api.meetup.com/2/events?group_id={groups}&status=upcoming,past&text_format=plain&order=time&omit=fee,photo_sample,rsvp_rules,rsvp_sample&fields=event_hosts".format(groups=",".join(groups))
run_import("events",eventUrl,importEvents,{})

result = session.run("MATCH (g:Group:Meetup)-[:CONTAINED]->(e:Event) WHERE NOT e:Past RETURN g.key as group, collect(toString(e.id)) as events")
for record in result:
    events = record["events"]
    while len(events) > 0:
        part = events[:100]
        events = events[100:]
        rsvpUrl="https://api.meetup.com/2/rsvps?event_id={events}&omit=member_photo,pay_status,venue,group,answers".format(events=",".join(part))
        run_import("rsvps "+record["group"],rsvpUrl,importRsvps,{})

result = session.run("MATCH (event:Event:Meetup) WHERE event.time < timestamp() SET event:Past")
print("set past event label",result.consume().counters)

session.close()
