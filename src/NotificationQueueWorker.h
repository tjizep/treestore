/*****************************************************************************

Copyright (c) 2000, 2013, Oracle and/or its affiliates. All rights reserved.
Copyright (c) 2008, 2009 Google Inc.
Copyright (c) 2009, Percona Inc.
Copyright (c) 2012, Facebook Inc.
Copyright (c) 2013, Christiaan Pretorius

Portions of this file contain modifications contributed and copyrighted by
Google, Inc. Those modifications are gratefully acknowledged and are described
briefly in the InnoDB documentation. The contributions by Google are
incorporated with their permission, and subject to the conditions contained in
the file COPYING.Google.

Portions of this file contain modifications contributed and copyrighted
by Percona Inc.. Those modifications are
gratefully acknowledged and are described briefly in the InnoDB
documentation. The contributions by Percona Inc. are incorporated with
their permission, and subject to the conditions contained in the file
COPYING.Percona.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin Street, Suite 500, Boston, MA 02110-1335 USA

*****************************************************************************/
#pragma once
#include <vector>
#include "Poco/Notification.h"
#include "Poco/NotificationQueue.h"
#include "Poco/ThreadPool.h"
#include "Poco/Thread.h"
#include "Poco/Runnable.h"
#include "Poco/Mutex.h"
#include "Poco/Random.h"
#include "Poco/AutoPtr.h"
namespace asynchronous{

	using Poco::Notification;
	using Poco::NotificationQueue;
	using Poco::ThreadPool;
	using Poco::Thread;
	using Poco::Runnable;
	using Poco::FastMutex;
	using Poco::AutoPtr;
	// quit notification send to worker thread
	class QuitNotification: public Notification
	{
	public:
		typedef AutoPtr<QuitNotification> Ptr;
	};

	template<typename _MessageType>
	class Worker: public Runnable
	{
	public:
		typedef _MessageType message_type;
		typedef Poco::AutoPtr<Worker> Ptr;
	public:
		Worker(const std::string& name, NotificationQueue& queue):
			_name(name),
			_queue(queue)
		{
		}
		virtual ~Worker(){
		}
		virtual void run()
		{
			Poco::Random rnd;
			for (;;)
			{
				Notification::Ptr pNf(_queue.waitDequeueNotification());
				if (pNf)
				{
					typename message_type::Ptr pWorkNf = pNf.cast<message_type>();
					if (pWorkNf)
					{
						pWorkNf->doTask();

						continue;
					}

					QuitNotification::Ptr pQuitNf = pNf.cast<QuitNotification>();
					break;

				}
				else
				{
					break;
				}
			}
		}

	private:
		std::string        _name;
		NotificationQueue& _queue;

	};


	template<typename _Notification>
	class QueueManager{
	public:
		typedef _Notification notification_type;
		typedef typename asynchronous::Worker<notification_type> _Worker;
		typedef _Worker* WorkerPtr;
		typedef std::vector<WorkerPtr> _Workers;
	protected:
		NotificationQueue queue;
		ThreadPool threads;
		_Workers workers;
		int last;
	protected:
		void createWorkers(){

			for (typename _Workers::iterator w = workers.begin(); w != workers.end(); ++w){
				(*w) = new _Worker("asynchronous worker", queue);
				threads.start(*(*w));
			}

		}
		void stopWorkers(){
			try{
				for (typename _Workers::iterator w = workers.begin(); w != workers.end(); ++w){
					queue.enqueueNotification(new QuitNotification());
				}
				/// printf("workers stopping...\n");
				/// os::zzzz(50);
				/// printf("workers stopped\n");
				for (typename _Workers::iterator w = workers.begin(); w != workers.end(); ++w){
					//delete (*w);
				}
			}catch(Poco::Exception&){
				printf("error stopping workers\n");
			}

		}
	public:
		explicit QueueManager(int w ) : threads(w,w*2), last(0){
			(*this).workers.resize(w);
			createWorkers();
		}
		QueueManager() : threads(2,2*2), last(0){
			(*this).workers.resize(2);
			createWorkers();
		}

		~QueueManager(){
			stopWorkers();
		}

		void add(typename QueueManager::notification_type* note){
			queue.enqueueNotification(note);

		}

	};
	class AbstractWorker : public Poco::Notification{
	public:
		AbstractWorker(){
		}
		void doTask(){
			work();
		}
		virtual void work() = 0;
		virtual ~AbstractWorker(){
		}
		typedef Poco::AutoPtr<AbstractWorker> Ptr;
	};
};
