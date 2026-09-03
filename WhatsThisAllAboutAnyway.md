# What Is This All About Anyway?

I know what you're thinking.. Does the world REALLY need Yet Another ESB?

Probably not, and this likely isn't the best one ever built.

So why all the trouble?

This project is an experiment in using an AI Coding Agent (Claude Code) for building a complex project. 

The initial brainstorm can be found in [Design Notes](./Design_Notes.md).

Things we are trying out:

## Claude as a Project Planner:

Claude builds tons of code, really fast, that humans simply cannot keep up with understand. Code reviews are monsterous and generally just blessed with "TGTM" as the reviewer shrugs their shoulders and hope the test automation covered everthing. The goal here is to keep the sessions interactive, with Claude assisting with planning and design. The project is broken into phases, with each having a separate detailed plan. Ultimately, we want to divide the project into small enough pieces that they could be handed off to a human developer. 

## Claude as an Architect

I've used Claude (and is some cases Gemini) to research several technologies to establish baseline architecture:

* Message Queing - Originally this project was called "GregorsServiceBus" but after some research, it was clear that Kafka has some limitation that made it less desirable. After some back and forth, I settled upon a messaging service called NATS Jetstream. (Hence the namechange to Jetcore.) Jetstream is written in Go instead of Java, so it compiles to machine code, not bytecode. It also doesn't have the issues with message sequencing that Kafka has when running parallel streams.
* Programming Language - Since the plan was put a lot of the heavy lifting on Claude, Claude and Gemini were both used to determine the optimal language for an AI Coding Agent. First choice was Python, so that was selected for this project. (Suprisingly, the second choice was Go, due to some features of how the language handles memory.)
* Workflow Automation and Data Transformation - Both of these were researched with goal of finding ways to precicely document them so that Claude can build a direct implementation instead of relying on pre-existing DSL runtimes. (For example, Claude can interpret a BPMN diagram and generate code to peform the steps defined.)
  + Workflow Automation - There are so many ways to do this. Right now the thinking is
    - Describe your workflow steps as clearly as possible in a markdown document.
    - All diagrams are built in Mermaid. (Claude can do this for you, but then you NEED to review!)
    - Use Claude to combine the documentation and diagrams and construct a program that executes the logic defined.
  + Data Transformation - This is a special subject that sits close to my heart. Swagger (OpenAPI) does a great job of defining data schemas, but it lacks when it comes to mapping between them.
    - [LinkML Map](https://linkml.io/linkml-map/) is a DSL for mapping data transformations between schemas. It has a DSL interpreter/runtime, but I believe we'll get better results by defining the mapping the in a clear format then allowing Claude to implement the mapping, without the overhead of a DSL runtime.

Claude as a programmer:

Claude will perform the coding tasks here but all code reviews and github commits are managed by humans. All [session logs](./.claude/session.log) are retained as part of this project so the interactions with Claude can be tracked over time.


_*This document is an overview of how Claude is used for this project. It should only be updated by humans.

