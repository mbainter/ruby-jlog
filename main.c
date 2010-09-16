#include "ruby.h"
#include "jlog.h"
#include "fcntl.h"

typedef struct {
   jlog_ctx *ctx;
   char *path;
   jlog_id start;
   jlog_id last;
   jlog_id prev;
   jlog_id end;
   int auto_checkpoint;
   int error;
} jlog_obj;

typedef jlog_obj * JLog;
typedef jlog_obj * JLog_Writer;
typedef jlog_obj * JLog_Reader;

VALUE cJLog;
VALUE cJLogWriter;
VALUE cJLogReader;
VALUE eJLog;

static void jlog_populate_subscribers(VALUE);

static void jlog_free(JLog jo) {
   if(jo->ctx) {
      jlog_ctx_close(jo->ctx);
   }

   if(jo->path) {
      free(jo->path);
   }

   free(jo);
}

static void jlog_raise(JLog jo, char* mess)
{
   VALUE e = rb_exc_new2(eJLog, mess);
   rb_iv_set(e, "error", INT2FIX(jlog_ctx_err(jo->ctx)));
   rb_iv_set(e, "errstr", rb_str_new2(jlog_ctx_err_string(jo->ctx)));
   rb_iv_set(e, "errno", INT2FIX(jlog_ctx_errno(jo->ctx)));

   rb_exc_raise(e);
}

static VALUE jlog_initialize(int argc, VALUE* argv, VALUE self)
{
   JLog jo;
   int options = O_CREAT;
   size_t size = 0;
   char *path = STR2CSTR(argv[0]);

   jo = ALLOC(jlog_obj);
   jo->ctx = jlog_new(path);
   jo->path = strdup(path);

   if(argc > 3) {
      options = FIX2INT(argv[3]);
      if(argc > 4) {
         size = NUM2INT(argv[4]);
      }
   }

   if(!jo->ctx) {
      jlog_free(jo);
      rb_raise(eJLog, "jlog_new(%s) failed", path);
   }

   if(options & O_CREAT) {
      if(jlog_ctx_init(jo->ctx) != 0) {
         if(jlog_ctx_err(jo->ctx) == JLOG_ERR_CREATE_EXISTS) {
            if(options & O_EXCL) {
               jlog_free(jo);
               rb_raise(eJLog, "file already exists: %s", path);
            }
         }else {
            jlog_raise(jo, "Error initializing jlog");
         }
      }
      jlog_ctx_close(jo->ctx);
      jo->ctx = jlog_new(path);
      if(!jo->ctx) {
         jlog_free(jo);
         rb_raise(eJLog, "jlog_new(%s) failed after successful init", path);
      }
   }

   VALUE tdata = Data_Wrap_Struct(self, 0, jlog_free, jo);
   VALUE subscribers = rb_ary_new();
   rb_iv_set(self, "@subscribers", subscribers);
   jlog_populate_subscribers(self);

/* Unneeded?
 * VALUE argv[0];
 * ...
 * rb_obj_call_init(tdata, 0, argv);
 */

   return tdata;
}

static VALUE jlog_add_subscriber(int argc, VALUE* argv, VALUE self)
{
   char *subscriber;
   long whence;
   JLog jo;

   Data_Get_Struct(self, jlog_obj, jo);

   subscriber = (NIL_P(argv[0]) ? NULL : StringValuePtr(argv[0]));
   whence = (NIL_P(argv[1]) ? 0 : NUM2LONG(argv[1]));

   if(!jo || !jo->ctx || jlog_ctx_add_subscriber(jo->ctx, subscriber, whence) != 0) {
      return Qfalse;
   }

   jlog_populate_subscribers(self);

   return Qtrue;
}


static VALUE jlog_remove_subscriber(VALUE self, VALUE subscriber)
{
   JLog jo;

   Data_Get_Struct(self, jlog_obj, jo);

   if(!jo || !jo->ctx || jlog_ctx_remove_subscriber(jo->ctx, STR2CSTR(subscriber)) != 0)
   {
      return Qfalse;
   }

   jlog_populate_subscribers(self);

   return Qtrue;
}

static void jlog_populate_subscribers(VALUE self)
{
   char **list;
   int i;
   JLog jo;
   VALUE subscribers;

   Data_Get_Struct(self, jlog_obj, jo);

   if(!jo || !jo->ctx)
   {
      rb_raise(eJLog, "Invalid jlog context");
   }

   subscribers = rb_iv_get(self, "@subscribers");

   jlog_ctx_list_subscribers(jo->ctx, &list);
   for(i=0; list[i]; i++ ) {
      rb_ary_push(subscribers, rb_str_new2(list[i]));
   }
   jlog_ctx_list_subscribers_dispose(jo->ctx, list);
}

static VALUE jlog_list_subscribers(VALUE self)
{
   JLog jo;
   VALUE subscribers;

   Data_Get_Struct(self, jlog_obj, jo);

   if(!jo || !jo->ctx)
   {
      rb_raise(eJLog, "Invalid jlog context");
   }

   subscribers = rb_iv_get(self, "@subscribers");


   return subscribers;
}

static VALUE jlog_raw_size2(VALUE self)
{
   size_t size;
   JLog jo;

   Data_Get_Struct(self, jlog_obj, jo);

   if(!jo || !jo->ctx) {
      rb_raise(eJLog, "Invalid jlog context");
   }
   size = jlog_raw_size(jo->ctx);

   return INT2NUM(size);
}

static VALUE jlog_close(VALUE self)
{
   JLog jo;

   Data_Get_Struct(self, jlog_obj, jo);

   if(!jo || !jo->ctx) return Qnil;

   jlog_ctx_close(jo->ctx);
   jo->ctx = NULL;

   return Qtrue;
}

static VALUE jlog_inspect(VALUE self)
{
   JLog jo;

   Data_Get_Struct(self, jlog_obj, jo);

   if(!jo || !jo->ctx) return Qnil;

   /* XXX Fill in inspect call data */

   return Qtrue;
}

static VALUE jlog_destroy(VALUE self)
{
   JLog jo;

   Data_Get_Struct(self, jlog_obj, jo);

   if(!jo) return Qnil;

   jlog_free(jo);

   return Qtrue;
}

static VALUE jlog_w_open(VALUE self)
{
   JLog jo;

   Data_Get_Struct(self, jlog_obj, jo);

   if(!jo || !jo->ctx) {
      rb_raise(eJLog, "Invalid jlog context");
   }

   if(jlog_ctx_open_writer(jo->ctx) != 0) {
      jlog_raise(jo, "jlog_ctx_open_writer failed");
   }

   return Qtrue;
}

static VALUE jlog_w_write(int argc, VALUE *argv, VALUE self)
{
   char *buffer;
   int ts = 0;
   jlog_message m;
   struct timeval t;
   long buffer_len;
   JLog jo;

   Data_Get_Struct(self, jlog_obj, jo);

   if(!jo || !jo->ctx) {
      rb_raise(eJLog, "Invalid jlog context");
   }

   if(argc > 3) {
      ts = (time_t) NUM2INT(argv[3]);
   }

   buffer = rb_str2cstr(argv[0], &buffer_len);
   m.mess = buffer;
   m.mess_len = buffer_len;
   t.tv_sec = ts;
   t.tv_usec = 0;

   if(jlog_ctx_write_message(jo->ctx, &m, ts ? &t : NULL) < 0) {
      return Qfalse;
   } else {
      return Qtrue;
   }
}


static VALUE jlog_r_open(VALUE self, VALUE subscriber)
{
   JLog jo;

   Data_Get_Struct(self, jlog_obj, jo);

   if(!jo || !jo->ctx) {
      rb_raise(eJLog, "Invalid jlog context");
   }

   if(jlog_ctx_open_reader(jo->ctx, STR2CSTR(subscriber)) != 0) {
      jlog_raise(jo, "jlog_ctx_open_reader failed");
   }

   return Qtrue;
}


static VALUE jlog_r_read(VALUE self)
{
   const jlog_id epoch = {0, 0};
   jlog_id cur;
   jlog_message message;
   int cnt;
   JLog jo;

   Data_Get_Struct(self, jlog_obj, jo);

   if(!jo || !jo->ctx) {
      rb_raise(eJLog, "Invalid jlog context");
   }

   // If start is unset, read the interval
   if(jo->error || !memcmp(&jo->start, &epoch, sizeof(jlog_id)))
   {
      jo->error = 0;
      cnt = jlog_ctx_read_interval(jo->ctx, &jo->start, &jo->end);
      if(cnt == 0 || (cnt == -1 && jlog_ctx_err(jo->ctx) == JLOG_ERR_FILE_OPEN)) {
         jo->start = epoch;
         jo->end = epoch;
         return Qnil;
      }
      else if(cnt == -1)
         jlog_raise(jo, "jlog_ctx_read_interval_failed");
   }

   // If last is unset, start at the beginning
   if(!memcmp(&jo->last, &epoch, sizeof(jlog_id))) {
      cur = jo->start;
   } else {
      // if we've already read the end, return; otherwise advance
      cur = jo->last;
      if(!memcmp(&jo->prev, &jo->end, sizeof(jlog_id))) {
         jo->start = epoch;
         jo->end = epoch;
         return Qnil;
      }
      jlog_ctx_advance_id(jo->ctx, &jo->last, &cur, &jo->end);
      if(!memcmp(&jo->last, &cur, sizeof(jlog_id))) {
            jo->start = epoch;
            jo->end = epoch;
            return Qnil;
      }
   }
   if(jlog_ctx_read_message(jo->ctx, &cur, &message) != 0) {
      if(jlog_ctx_err(jo->ctx) == JLOG_ERR_FILE_OPEN) {
         jo->error = 1;
         return Qnil;
      }

      // read failed; raise error but recover if read is retried
      jo->error = 1;
      jlog_raise(jo, "read failed");
   }
   if(jo->auto_checkpoint) {
      if(jlog_ctx_read_checkpoint(jo->ctx, &cur) != 0)
         jlog_raise(jo, "checkpoint failed");

      // must reread the interval after a checkpoint
      jo->last = epoch;
      jo->prev = epoch;
      jo->start = epoch;
      jo->end = epoch;
   } else {
      // update last
      jo->prev = jo->last;
      jo->last = cur;
   }

   /* XXX
    * ruby_array = [message.mess, message.mess_len]
    * return ruby_array
    */
   return Qnil;
}


static VALUE jlog_r_rewind(VALUE self)
{
   JLog jo;

   Data_Get_Struct(self, jlog_obj, jo);

   if(!jo || !jo->ctx) {
      rb_raise(eJLog, "Invalid jlog context");
   }

   jo->last = jo->prev;

   return Qtrue;
}


static VALUE jlog_r_checkpoint(VALUE self)
{
   jlog_id epoch = { 0, 0 };
   JLog jo;

   Data_Get_Struct(self, jlog_obj, jo);

   if(!jo || !jo->ctx) {
      rb_raise(eJLog, "Invalid jlog context");
   }

   if(memcmp(&jo->last, &epoch, sizeof(jlog_id)))
   {
      jlog_ctx_read_checkpoint(jo->ctx, &jo->last);

      // re-read the interval
      jo->last = epoch;
      jo->start = epoch;
      jo->end = epoch;
   }

   return Qtrue;
}


static VALUE jlog_r_auto_checkpoint(int argc, VALUE *argv, VALUE self)
{
   JLog jo;

   Data_Get_Struct(self, jlog_obj, jo);

   if(!jo || !jo->ctx) {
      rb_raise(eJLog, "Invalid jlog context");
   }

   if(argc > 1) {
      int ac = argv[1];
      jo->auto_checkpoint = !!ac;
   }

   return INT2FIX(jo->auto_checkpoint);
}



void Init_jlog(void) {
   cJLog = rb_define_class("JLog", rb_cObject);
   cJLogWriter = rb_define_class_under(cJLog, "Writer", rb_cObject);
   cJLogReader = rb_define_class_under(cJLog, "Reader", rb_cObject);

   eJLog = rb_define_class_under(cJLog, "Error", rb_eStandardError);

   rb_define_global_const("JLogReader", cJLogReader);
   rb_define_global_const("JLogWriter", cJLogWriter);
   rb_define_global_const("JLogError", eJLog);

   rb_define_method(cJLog, "initialize", jlog_initialize, -1);

   rb_define_method(cJLog, "add_subscriber", jlog_add_subscriber, -1);
   rb_define_method(cJLog, "remove_subscriber", jlog_remove_subscriber, 1);
   rb_define_method(cJLog, "list_subscribers", jlog_list_subscribers, 0);
   rb_define_method(cJLog, "raw_size", jlog_raw_size2, 1);
   rb_define_method(cJLog, "close", jlog_close, 0);
   rb_define_method(cJLog, "destroy", jlog_destroy, 0);
   rb_define_method(cJLog, "inspect", jlog_inspect, 0);

   rb_define_alias(cJLog, "size", "raw_size");

   rb_define_method(cJLogWriter, "open", jlog_w_open, 0);
   rb_define_method(cJLogWriter, "write", jlog_w_write, -1);

   rb_define_method(cJLogReader, "open", jlog_r_open, 1);
   rb_define_method(cJLogReader, "read", jlog_r_read, 0);
   rb_define_method(cJLogReader, "rewind", jlog_r_rewind, 0);
   rb_define_method(cJLogReader, "checkpoint", jlog_r_checkpoint, 0);
   rb_define_method(cJLogReader, "auto_checkpoint", jlog_r_auto_checkpoint, 0);
}


/* ErrorType message, p 
 * message "; error: %d (%s) errno: %d (%s)",
 * jlog_ctx_err(p->ctx), jlog_ctx_err_string(p->ctx), jlog_ctx_errno(p->ctx), strerror(jlog_ctx_errno(j->ctx))
*/

/*
rb_define_class_under(cJLog, *[define class args])

   
void  	rb_define_method(VALUE classmod, char *name, VALUE(*func)(), int argc")
  	Defines an instance method in the class or module classmod with the given name, implemented by the C function func and taking argc arguments.
void  	rb_define_module_function(VALUE classmod, char *name, VALUE(*func)(), int argc)")
  	Defines a method in class classmod with the given name, implemented by the C function func and taking argc arguments.
void  	rb_define_global_function(char *name, VALUE(*func)(), int argc")
  	Defines a global function (a private method of Kernel) with the given name, implemented by the C function func and taking argc arguments.
   */
