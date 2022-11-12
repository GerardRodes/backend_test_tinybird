# TinyHTTPDataHandlerServer

TinyHTTPDataHandlerServer is an HTTP server that accepts [NDJSON](http://ndjson.org/) data for taxi trips, see
[nyc_taxi.ndjson](nyc_taxi.ndjson) for a sample, picks some attributes for each JSON value, transform them to CSV and
store them in a per-day CSV file in the FS.

The application is based on the [Tornado web framework](https://www.tornadoweb.org/en/stable/guide/intro.html), and
although the server is far from being production ready, let's imagine it is already deployed and running in production.

We already know that the application does not perform well when concurrency increases.

See the [BUILD.md](BUILD.md) for more details about how to install & run the application and benchmark the performance.

## Assumptions

  - You should read the whole README.md and have a look at the code before you start.
  - Feel free to ask any questions at any moment, but it's better if you ask before you dive into making changes.
  - If some answer is unclear and blocking you, take a guess, write down the assumption and continue working.
  - The code will run under a recent Linux distribution.
  - You can add or change any dependency.
  - You can use as many resources as you want from the host machine. There is no need to control or limit them manually.

## Goal

You might be familiar with Python, Async IO, and Tornado or some of them—or you never worked with any of them. Don't
worry, if you are not familiar with them, we don't expect you to do a lot of code changes in the application.

There are many improvements to be done, we certainly don't need you to do everything (or anything specific). Instead, we
want you to describe the problems you find, what alternatives and changes you would consider, what could be done and, if
possible, implement it up to certain point.

## Deliverables

You final solution should include:
  - Any code changes or additions done to the current project.
  - A [Markdown](https://en.wikipedia.org/wiki/Markdown) file justifying the decisions made during the development and
  what other improvements could be made to the project.

You can provide the solution as a link to a repository (preferable) or as a series of patches.

## Evaluation

We expect you to spend 2-3 hours on the exercise. If you go over it, just write down what it's done and what's pending.
The explanation is as important as the code changes, so take some of that time to write things down.

We expect you to send us a solution within a week, but if you need more time because of personal matters, let us know.

We'll evaluate **your explanation** based on:
  - **Clarity**: Developers that haven't seen the project before should be able to understand your reasoning.
  - **Decisions**: Explain **why** you took a decision. Even if it's random or exploratory, we want to know.
  - **Other approaches**: We want to know also about the options you discarded and why.
  - **Design**: Schema design, data processing decisions, algorithms, tradeoffs and so on.
  - **Evolution**: How should the project be improved further in your opinion: Next steps, things to consider in the
  future...
  - When in doubt, it's better to be verbose and over-document things.

We'll evaluate **your code changes** based on:
  - Simplicity: Are the changes clear?
  - Stability and reproducibility. Are there any bugs? Does it always return the same value for the same input?
  - Design, processing flow, etc. Is it using the right tool for the right use case?
  - Performance and resource usage. You have infinite resources available but that doesn't mean you need to use them.
