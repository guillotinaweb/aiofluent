1.2.1 (unreleased)
------------------

- Add support for nanosecond precision timestamps
  [davidonna]

1.2.0 (2018-06-14)
------------------

- Maintain one AsyncIO queue for all logs
  [vangheem]

1.1.4 (2018-05-29)
------------------

- Handle RuntimeError on canceling tasks/cleanup
  [vangheem]


1.1.3 (2018-02-15)
------------------

- Lock calling the close method of sender
  [vangheem]

- Increase default timeout
  [vangheem]


1.1.2 (2018-02-07)
------------------

- lock the whole method
  [vangheem]


1.1.1 (2018-02-07)
------------------

- Use lock on getting connection object
  [vangheem]


1.1.0 (2018-01-25)
------------------

- Move to using asyncio connection infrastructure instead of sockets
  [vangheem]


1.0.8 (2018-01-04)
------------------

- Always close out buffer data
  [vangheem]


1.0.7 (2018-01-04)
------------------

- Handle errors processing log queue
  [vangheem]


1.0.6 (2017-11-14)
------------------

- Prevent log queue from getting too large
  [vangheem]


1.0.5 (2017-10-17)
------------------

- Fix release to include CHANGELOG.rst file
  [vangheem]


1.0.4 (2017-10-10)
------------------

- Fix pushing initial record


1.0.3 (2017-10-10)
------------------

- Handle Runtime error when logging done before event loop started
  [vangheem]


1.0.2 (2017-10-09)
------------------

- Fix to make normal logging call async
  [vangheem]


1.0.1 (2017-07-03)
------------------

- initial release
