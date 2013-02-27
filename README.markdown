mData
=====

![Screenshot](http://colorissimo.net/img/mdata0001.jpg)

How to use/install?
===================

Just take the sources, compile and have a fun. No 3rd parties, no external libraries/frameworks... All you need is .NET and patience to read all below...

Note: I have a lot of sources for testing this stuff, from testing the trees to testing multithreaded read/write concurrency, etc., etc. I can add them by request.

What is this?
=============

mData is a .NET library which provides following more or less isolated pieces of functionality:

+	Common array-based data structures. It includes array-based red-black tree, array-based paged list, page-based red-black tree, which can be easily cached and key-value collection of two ints.

+	Generic binary-serializable DataValue support (see below)

+	Expression compilation and execution. Expressions are based on DataValues and can be used separately, for example for customization purposes.

+	Miltithreaded storage (technically network model database) which allow saving, store and extracting DataValues.

+	Columnar DataValue based result set and its processing. It can be used separately. This result set (QueryResult) supports proprietary query language named LISQ (see test example).

What was a reason to do it?
===========================

I can list following reasons:

+	__Fun of the 'reinventing the bicycle'.__ Yes! We can discuss a lot of things here, but being a lucky guy who have a programming as a job and as a hobby, I just like it!

+	__Just for fun and trying to make the things simple.__ Yes, there are a lot of the similar things around... But (IMHO) we need something more simple and more completed. For example, some implementations of database contexts around are single-threaded. I can only guess how they can be used in real server applications. Some implementations missing the resource management functionality (e,g, doesn't control memory consumed, threads usage, etc.). So I just have tried to give my own solution for this.

+	__Playing with possible architecture solutions.__ It's not a secret that sometimes simple solutions are over engineered and stuffed with unneeded abstraction layers. For example, pure Dependency Injection (again IMHO) is a solely matter of architecture, it can't appear just from bringing external frameworks in your application. So I have tried approach when functionality is sealed inside a library, and the only external things are interfaces and factory(ies) which create another interface implementations based (maybe) on another existing implementations... No direct class instantiation. This is an item to discuss, but in a complex application I see this approach the only really applicable for isolating existing technical layers.

+	__I have some experience in compilation and code generation, so here is just another one.__ Sometimes we need something simple to have ability to evaluate some expression just inside our code. This expression can be stored in configuration storage or in a database. I found that we are missing such things... Really, you can include an external interpreter, struggle with passing context forth and back, apply a bloated configuration to it, etc., etc. But sometimes we need some just simple. Just 'compile me that' and 'execute it for me', nothing more. I have tried to provide something like this.

+	__We are missing really powerful table data processing for implementation of business logic.__ Yes it is... Again, sometimes task is quite simple, but in a real code it appears a nightmare. All we need is just to provide appropriate formalization level. I think I was more or less successful in this. And again I have tried to keep the things simple. All I have implemented is only one class: QueryResult. And I believe that we don't need something else in 80% of cases.

DataValue
=========

What is the DataValue:

+	__Just a way to store of variables of different types.__ You can ask: 'We already have boxing, dynamics, etc... Why we need another one?'. Answer will be following: 'I want to avoid boxing where possible and make development/testing process more adequate'. This is an item for a separate discussion, but imagine the problems appears when you will try to implement container which can store just only existing scalar types using boxing...

+	__It is binary serializable.__ Serialization is another problem usually appears when you trying to make your data persistent. What to serialize, how to serialize? Answers on such questions are very vary, including external serialization frameworks, which are usually too generic, and sometimes too bloated and too academic... I have decided to implement something simple, fast and fail-safe as possible (look on the usage of signatures in code). My serialization/deserialization interfaces contains only 4 methods each.

+	__It provides a layer of isolation.__ In a different meanings... It can be stored in a database (not an abstract object), it can be serialized/deserialized, it can be used as context, parameters and result for an expression layer, it is a way for data exchange for QueryResult. Not an abstract/base class, just it. And this is I am calling 'isolation layer'.

To be continued...
==================

Author
------

Denis Mitrofanov


