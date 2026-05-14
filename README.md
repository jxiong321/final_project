Problems:

- Q1: CSVSOurce needs headers. how should I specify them? especially if I'm going to apply transforms where the user wants to create NEW rows, in which case the sink headers are not the same as the source headers. As well, two sources may have different headers. James said one thing to do is have the USER specify what the Sink headers should be, and if anything is wrong, then it's an issue with the transforms that THEY wrote.

I think that makes sense. However, it's difficult to check this, and differentiate between WHY the value is missing some headers in the row. Because it could be missing headers because the user misspecified the functions and tranforms, but it could ALSO be missing because there's just an issue w the data and some transforms are being dropped. i guess i could just leave it there since both are issues w the user, but yeah.

Perhaps i should just write it to the error sink yeah... since it's hard to catch.

also, if the user specifies on the sink, what do i do if they are out of order? how do i rearrange the code lol?

- Q2: another edit i talked through w James is that it might be good to have TWO sinks: one for ones that errored, and one for rows that are good. This way, it doesn't crash and the users can manually inspect the errord ones later and figure it out themselves.

- i'm also struggling to specify what exactly an error looks like, and why it would happen, so i can understand how to catch it. 
the only error for the transforms i can really think of, is just a type mismatch. for example the input is wrong or something. 



Edit 2:
-Q1 Decision : put the burden on the user. Can start with lenient mode: just write the missing columns as an empty string and move on. then maybe can consider making it configurable 

- Q2 Decision: do it. make error sink optional. Otherwise just drop the records. 

Q3: Identify the errors

User errors:
- bad CSV/JSONL path 
- bad transform 
- bad add_source 

Data errors:
- missing values (just ignore for now)
- wrong input type for fields (where to validate?) -> error sink w a note
    - make the transforms a Try Except kind of thing.

Environmental errors:
- ignore for now lol

HOW TO VALIDATE:
- for each TRANSFORM. User must specify INPUT FIELDS AND THEIR TYPE.

# before
@pipeline.transform(input_fields=["age"], kind="cleaning")
def age_to_int(record):
    record["age"] = int(record["age"])
    return record

# after
@pipeline.transform(input_fields={"age": str}, kind="cleaning")
def age_to_int(record):
    record["age"] = int(record["age"])
    return record